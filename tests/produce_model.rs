//! Model checking of the idempotent producer's protocol with a broker (`src/producer.rs`): every
//! state reachable in a small configuration, explored breadth first, each one checked against the
//! invariants an application of an idempotent producer rests on: nothing written twice or out of
//! the order sent, each message reported at the offset it is at, a message reported failed and
//! not persisted never on the log, and one written but reported failed only with the producer
//! fatal (an application that stops on a failed delivery then never loses track of the log).
//!
//! The model is one partition: the application sending N messages, their deadlines passing in the
//! order they were sent (`message.timeout.ms` is the same for every message), the producer's rules,
//! and a broker keeping Kafka's idempotence state for the producer id. Every interleaving is
//! explored: a batch sealed of any number of the queued messages; a request or its answer lost (a
//! connection dropped, a client timeout); a retriable refusal, either before the broker appended
//! the batch (NOT_ENOUGH_REPLICAS) or after (REQUEST_TIMED_OUT, NOT_ENOUGH_REPLICAS_AFTER_APPEND);
//! a refusal for good of the batch (MESSAGE_TOO_LARGE, TOPIC_AUTHORIZATION_FAILED: never appended);
//! a batch sent again answered either as Kafka does for its last batches (NONE, the offset it was
//! written at) or as DUPLICATE_SEQUENCE_NUMBER.
//!
//! Each action of the model is a rule of the code, named by the function it models:
//! - `Sent::maybe_written`: a batch may have been written once a request of it was written and its
//!   answer never had, or it was answered with a code a broker gives after appending it.
//! - `expire`: queued messages past their deadline time out; a batch neither in a request nor
//!   answered (`!sending`) past its first message's deadline times out: if it may have been
//!   written, the producer is fatal (its sequence may now have a gap); else its sequence is the
//!   next batch's.
//! - `fail_queued`: fatal, what is queued fails, and a batch not in a request.
//! - `take_due`: one batch of a partition at a time; a new one only when none is in flight and the
//!   producer is not fatal; a batch not answered is sent again with the same sequence.
//! - `send`: a request is not sent past its first message's deadline (given up as timed out).
//! - `applied`: NONE or DUPLICATE_SEQUENCE_NUMBER delivers (DUPLICATE at no offset), a retriable
//!   code sends the batch again, a refusal for good fails it: alone, its sequence the next batch's,
//!   if it may not have been written; else with the producer fatal; a refusal of the sequence
//!   (OUT_OF_ORDER_SEQUENCE_NUMBER) fails it with the producer fatal.
//!
//! What the model leaves out: other partitions and brokers (each partition's batches are
//! independent in the code: one in flight a partition), the producer id (fixed), leaders moving
//! (the broker's log and idempotence state are the partition's, whoever leads it), sequences
//! wrapping past i32::MAX, and time beyond the order of the deadlines.
#![allow(clippy::unit_arg)] // an action is `Ok(` the state change it makes `)`
use std::collections::{HashSet, VecDeque};

/// What became of a message, as `poll` reports it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Outcome {
    /// delivered at this offset, or (DUPLICATE_SEQUENCE_NUMBER) at one the broker did not say
    Delivered(Option<u8>),
    /// timed out; true: reported possibly persisted (`Persisted::Possibly`), false: not
    TimedOut(bool),
    /// failed for good (a code a retry does not mend, or the producer fatal); as `TimedOut`
    Failed(bool),
    /// refused by `send`: the producer was fatal already
    Refused,
}

/// Where a sealed batch is: to be sent (taken by a broker thread), on the wire, answered.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Wire {
    /// taken (`sending`), not yet written: `send` checks the deadline first
    Taken,
    /// written; the broker has not read it
    Request,
    /// the broker's answer, on its way back
    Answer(Answer),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Answer {
    /// NONE: written at this base offset
    Written(u8),
    /// DUPLICATE_SEQUENCE_NUMBER: written before, its offset not said
    Duplicate,
    /// a code a retry may mend, which the broker gives before appending: not written
    Retriable,
    /// a code a retry may mend, which the broker gives after appending: written
    RetriableWritten,
    /// a refusal of the batch for good (MESSAGE_TOO_LARGE and the like): not written
    Refused,
    /// OUT_OF_ORDER_SEQUENCE_NUMBER: a sequence past the broker's next
    OutOfOrder,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Batch {
    msgs: Vec<u8>,
    seq: u8,
    /// None: not in a request (`!sending`), to be sent again
    wire: Option<Wire>,
    /// `Sent::maybe_written`
    maybe_written: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct State {
    /// messages the application has sent: 0..sent
    sent: u8,
    /// messages whose deadline has passed: 0..expired (in the order sent)
    expired: u8,
    queue: VecDeque<u8>,
    inflight: Option<Batch>,
    next_seq: u8,
    fatal: bool,
    outcome: Vec<Option<Outcome>>,
    /// the broker: the partition's log (message ids), the producer id's next sequence and its
    /// last batch (its first sequence, base offset)
    log: Vec<u8>,
    b_next: u8,
    b_last: Option<(u8, u8)>,
    /// requests the client gave up on (a timeout, a connection closed) that the broker may read
    /// all the same, later: a batch's messages and sequence; at most `GHOSTS`
    ghosts: Vec<(Vec<u8>, u8)>,
}

/// How many requests given up may be on their way to the broker at once.
const GHOSTS: usize = 2;

/// A state reached by an action, or the invariant that action broke.
type Step = Result<State, String>;

impl State {
    fn new(n: u8) -> State {
        State {
            sent: 0,
            expired: 0,
            queue: VecDeque::new(),
            inflight: None,
            next_seq: 0,
            fatal: false,
            outcome: vec![None; n as usize],
            log: vec![],
            b_next: 0,
            b_last: None,
            ghosts: vec![],
        }
    }

    fn report(&mut self, m: u8, o: Outcome) -> Result<(), String> {
        let slot = &mut self.outcome[m as usize];
        if let Some(before) = slot {
            return Err(format!("message {m} reported twice: {before:?}, then {o:?}"));
        }
        *slot = Some(o);
        Ok(())
    }

    /// The broker reads a batch: written if its sequence is the next, the base offset it was
    /// written at if it is the last batch again, else refused (OUT_OF_ORDER_SEQUENCE_NUMBER).
    fn broker_reads(&mut self, msgs: &[u8], seq: u8) -> Answer {
        if seq == self.b_next {
            let base = self.log.len() as u8;
            self.log.extend(msgs);
            (self.b_next, self.b_last) = (seq + msgs.len() as u8, Some((seq, base)));
            Answer::Written(base)
        } else if self.b_last.is_some_and(|l| l.0 == seq) {
            Answer::Written(self.b_last.unwrap().1) // Kafka: its last batches answered again
        } else {
            Answer::OutOfOrder
        }
    }

    /// `expire`: what is past its deadline given up.
    fn expire(&mut self) -> Result<(), String> {
        while self.queue.front().is_some_and(|m| *m < self.expired) {
            let m = self.queue.pop_front().unwrap();
            self.report(m, Outcome::TimedOut(false))?;
        }
        if self.inflight.as_ref().is_some_and(|b| b.wire.is_none() && b.msgs[0] < self.expired) {
            let b = self.inflight.take().unwrap();
            for m in b.msgs {
                self.report(m, Outcome::TimedOut(b.maybe_written))?;
            }
            match b.maybe_written {
                true => self.fatal = true,
                false => self.next_seq = b.seq,
            }
        }
        Ok(())
    }

    /// Every state one action leads to.
    fn next(&self, n: u8) -> Vec<Step> {
        let mut out: Vec<Step> = vec![];
        let go = |out: &mut Vec<Step>, f: &dyn Fn(&mut State) -> Result<(), String>| {
            let mut s = self.clone();
            out.push(f(&mut s).map(|()| s));
        };
        // the application sends its next message (`Producer::send`)
        if self.sent < n {
            go(&mut out, &|s| {
                let m = s.sent;
                s.sent += 1;
                match s.fatal {
                    true => s.report(m, Outcome::Refused),
                    false => Ok(s.queue.push_back(m)),
                }
            });
        }
        // the oldest pending deadline passes
        if self.expired < self.sent {
            go(&mut out, &|s| Ok(s.expired += 1));
        }
        // the broker reads a request the client gave up on: its answer goes to no one
        for i in 0..self.ghosts.len() {
            go(&mut out, &move |s: &mut State| {
                let (msgs, seq) = s.ghosts.remove(i);
                s.broker_reads(&msgs, seq);
                Ok(())
            });
        }
        // `expire`, which `poll`, `take_due` and the producer's thread each run: only an action
        // when it gives something up
        {
            let mut s = self.clone();
            match s.expire() {
                Err(e) => out.push(Err(e)),
                Ok(()) if s != *self => out.push(Ok(s)),
                Ok(()) => {}
            }
        }
        // `fail_queued`
        if self.fatal && (!self.queue.is_empty() || self.inflight.as_ref().is_some_and(|b| b.wire.is_none())) {
            go(&mut out, &|s| {
                while let Some(m) = s.queue.pop_front() {
                    s.report(m, Outcome::Failed(false))?;
                }
                if s.inflight.as_ref().is_some_and(|b| b.wire.is_none()) {
                    let b = s.inflight.take().unwrap();
                    for m in b.msgs {
                        s.report(m, Outcome::Failed(b.maybe_written))?;
                    }
                }
                Ok(())
            });
        }
        // `take_due` (after its own `expire`, explored above as an action of its own): not fatal
        if !self.fatal {
            match &self.inflight {
                // sent again, with its sequence
                Some(b) if b.wire.is_none() => {
                    go(&mut out, &|s| Ok(s.inflight.as_mut().unwrap().wire = Some(Wire::Taken)))
                }
                Some(_) => {}
                // a new batch of the first k queued messages, any k
                None => {
                    for k in 1..=self.queue.len() {
                        go(&mut out, &move |s: &mut State| {
                            let msgs: Vec<u8> = s.queue.drain(..k).collect();
                            let seq = s.next_seq;
                            s.next_seq += k as u8;
                            s.inflight = Some(Batch { msgs, seq, wire: Some(Wire::Taken), maybe_written: false });
                            Ok(())
                        });
                    }
                }
            }
        }
        let Some(b) = &self.inflight else { return out };
        match b.wire {
            // `send`: past its first message's deadline, not written (given up as timed out)
            Some(Wire::Taken) => go(&mut out, &|s| {
                let b = s.inflight.as_mut().unwrap();
                if b.msgs[0] < s.expired {
                    b.wire = None;
                    return Ok(());
                }
                if let Some(m) = b.msgs.iter().find(|m| **m < s.expired) {
                    return Err(format!("message {m} sent past its deadline"));
                }
                b.wire = Some(Wire::Request);
                Ok(())
            }),
            Some(Wire::Request) => {
                // the broker reads it, as Kafka's idempotence keeps a producer's sequences, and
                // answers as it wrote it, or with a retriable code it gives after appending
                for after_append in [false, true] {
                    go(&mut out, &move |s: &mut State| {
                        let (msgs, seq) = (b.msgs.clone(), b.seq);
                        let answer = s.broker_reads(&msgs, seq);
                        if answer == Answer::OutOfOrder {
                            // the producer sends a batch at the broker's next sequence or again at
                            // its last: never past it, so its own requests never find a gap
                            return Err(format!("batch {msgs:?} at sequence {seq}: a gap, the broker at {}", s.b_next));
                        }
                        let answer = if after_append { Answer::RetriableWritten } else { answer };
                        s.inflight.as_mut().unwrap().wire = Some(Wire::Answer(answer));
                        Ok(())
                    });
                }
                // ... or it answers DUPLICATE_SEQUENCE_NUMBER to a batch written before
                if self.b_last.is_some_and(|l| l.0 == b.seq) {
                    go(&mut out, &|s| Ok(s.inflight.as_mut().unwrap().wire = Some(Wire::Answer(Answer::Duplicate))));
                }
                // ... or refuses it, unwritten, for now or for good (whatever it wrote before)
                for a in [Answer::Retriable, Answer::Refused] {
                    go(&mut out, &move |s: &mut State| Ok(s.inflight.as_mut().unwrap().wire = Some(Wire::Answer(a))));
                }
                // ... or the request is lost (the connection dropped, the client timed out)
                go(&mut out, &|s| {
                    let b = s.inflight.as_mut().unwrap();
                    (b.wire, b.maybe_written) = (None, true);
                    Ok(())
                });
                // ... or the client gives up on it (a timeout) while the broker may still read it
                if self.ghosts.len() < GHOSTS {
                    go(&mut out, &|s| {
                        let b = s.inflight.as_mut().unwrap();
                        (b.wire, b.maybe_written) = (None, true);
                        let ghost = (b.msgs.clone(), b.seq);
                        s.ghosts.push(ghost);
                        Ok(())
                    });
                }
            }
            Some(Wire::Answer(a)) => {
                // the answer is lost
                go(&mut out, &|s| {
                    let b = s.inflight.as_mut().unwrap();
                    (b.wire, b.maybe_written) = (None, true);
                    Ok(())
                });
                // `applied`
                go(&mut out, &move |s: &mut State| {
                    match a {
                        Answer::Written(base) => {
                            for (i, m) in s.inflight.take().unwrap().msgs.into_iter().enumerate() {
                                s.report(m, Outcome::Delivered(Some(base + i as u8)))?;
                            }
                        }
                        Answer::Duplicate => {
                            for m in s.inflight.take().unwrap().msgs {
                                s.report(m, Outcome::Delivered(None))?;
                            }
                        }
                        Answer::Retriable => s.inflight.as_mut().unwrap().wire = None,
                        Answer::RetriableWritten => {
                            let b = s.inflight.as_mut().unwrap();
                            (b.wire, b.maybe_written) = (None, true);
                        }
                        Answer::Refused => {
                            let b = s.inflight.take().unwrap();
                            for m in b.msgs {
                                s.report(m, Outcome::Failed(b.maybe_written))?;
                            }
                            match b.maybe_written {
                                true => s.fatal = true,
                                false => s.next_seq = b.seq,
                            }
                        }
                        Answer::OutOfOrder => {
                            let b = s.inflight.take().unwrap();
                            for m in b.msgs {
                                s.report(m, Outcome::Failed(b.maybe_written))?;
                            }
                            s.fatal = true;
                        }
                    }
                    Ok(())
                });
            }
            None => {}
        }
        out
    }

    /// The invariants, in every state.
    fn check(&self) -> Result<(), String> {
        let mut seen = HashSet::new();
        if let Some(m) = self.log.iter().find(|m| !seen.insert(**m)) {
            return Err(format!("message {m} written twice: {:?}", self.log));
        }
        if !self.log.windows(2).all(|w| w[0] < w[1]) {
            return Err(format!("written out of the order sent: {:?}", self.log));
        }
        for (m, o) in self.outcome.iter().enumerate() {
            let m = m as u8;
            let written = self.log.iter().position(|x| *x == m);
            match o {
                Some(Outcome::Delivered(Some(at))) if written != Some(*at as usize) => {
                    return Err(format!("message {m} reported at offset {at}, written at {written:?}"));
                }
                Some(Outcome::Delivered(None)) if written.is_none() => {
                    return Err(format!("message {m} reported delivered, not written"));
                }
                // written but reported failed: only with the producer fatal (an application that
                // stops on a failed delivery then starts again from what is on the log)
                Some(Outcome::TimedOut(_) | Outcome::Failed(_)) if written.is_some() && !self.fatal => {
                    return Err(format!("message {m} written, reported failed, the producer not fatal"));
                }
                // reported not persisted: not on the log, nor ever (checked in every state after)
                Some(Outcome::TimedOut(false) | Outcome::Failed(false) | Outcome::Refused) if written.is_some() => {
                    return Err(format!("message {m} written, reported not persisted"));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// The hard cases, each reached somewhere (else the model would prove little): a delivery at no
/// offset, a message written by a request the client gave up on, a message written and reported
/// timed out (the producer fatal), a batch sent again and answered with its first write's offset,
/// a batch failed alone (refused, or timed out unwritten) and a later message delivered at its
/// sequence, the producer not fatal.
#[derive(Default, Debug)]
struct Witnessed {
    duplicate: bool,
    ghost_written: bool,
    written_timed_out: bool,
    resent_delivered: bool,
    failed_alone_then_delivered: bool,
}

/// Every state reachable with `n` messages: how many, and the hard cases reached; or the first
/// invariant broken and the state that led to it.
fn explore(n: u8) -> Result<(usize, Witnessed), String> {
    let start = State::new(n);
    let mut seen: HashSet<State> = HashSet::from([start.clone()]);
    let mut todo = VecDeque::from([start]);
    let mut w = Witnessed::default();
    while let Some(s) = todo.pop_front() {
        s.check().map_err(|e| format!("{e}\n  in {s:?}"))?;
        for (m, o) in s.outcome.iter().enumerate() {
            let written = s.log.contains(&(m as u8));
            w.duplicate |= *o == Some(Outcome::Delivered(None));
            w.written_timed_out |= written && matches!(o, Some(Outcome::TimedOut(_)));
        }
        // a batch's message failed alone, a later one at the head of the log
        let alone = |o: &Option<Outcome>| matches!(o, Some(Outcome::Failed(false) | Outcome::TimedOut(false)));
        w.failed_alone_then_delivered |=
            !s.fatal && s.log.first().is_some_and(|f| (0..*f as usize).any(|m| alone(&s.outcome[m])));
        w.resent_delivered |= matches!(&s.inflight, Some(b) if b.seq < s.b_next
            && matches!(b.wire, Some(Wire::Answer(Answer::Written(_)))));
        let next = s.next(n);
        // nothing left to do: every message sent has its outcome (none is lost, none hangs)
        if next.is_empty() {
            if let Some(m) = s.outcome.iter().position(Option::is_none) {
                return Err(format!("the end, message {m} without an outcome\n  in {s:?}"));
            }
        }
        for t in next {
            let t = t.map_err(|e| format!("{e}\n  from {s:?}"))?;
            w.ghost_written |= t.ghosts.len() < s.ghosts.len() && t.log.len() > s.log.len();
            if seen.insert(t.clone()) {
                todo.push_back(t);
            }
        }
    }
    Ok((seen.len(), w))
}

#[test]
fn the_idempotent_protocol_keeps_its_invariants_in_every_state() {
    for n in 1..=5 {
        let (states, w) = explore(n).unwrap_or_else(|e| panic!("{n} messages: {e}"));
        eprintln!("{n} messages: {states} states, each checked; {w:?}");
        let all = w.duplicate && w.ghost_written && w.written_timed_out && w.resent_delivered;
        assert!(all && (n < 2 || w.failed_alone_then_delivered), "{n}: {w:?}");
    }
}

/// The first invariant broken where `next` is replaced by `broken` (3 messages), if one is.
fn first_broken(broken: impl Fn(&State) -> Vec<Step>) -> Option<String> {
    let start = State::new(3);
    let (mut seen, mut todo) = (HashSet::from([start.clone()]), VecDeque::from([start]));
    while let Some(s) = todo.pop_front() {
        if let Err(e) = s.check() {
            return Some(e);
        }
        for t in broken(&s).into_iter().flatten() {
            if seen.insert(t.clone()) {
                todo.push_back(t);
            }
        }
    }
    None
}

/// The checker finds what it checks for. A batch past its deadline given up without making the
/// producer fatal (the broker may have written it) is caught; so is a batch sent again under a new
/// sequence rather than its own (written twice); so is a batch's sequence given to the next batch
/// though it may have been written (the producer forgetting `maybe_written`).
#[test]
fn the_checker_catches_a_broken_rule() {
    let e = first_broken(|s| {
        let mut out = s.next(3);
        for t in out.iter_mut().flatten() {
            if s.inflight.as_ref().is_some_and(|b| b.wire.is_none()) && t.inflight.is_none() && t.fatal && !s.fatal {
                t.fatal = false;
            }
        }
        out
    });
    assert!(e.as_deref().is_some_and(|e| e.contains("written, reported failed, the producer not fatal")), "{e:?}");
    let e = first_broken(|s| {
        let mut out = s.next(3);
        for t in out.iter_mut().flatten() {
            let resent = s.inflight.as_ref().is_some_and(|b| b.wire.is_none())
                && t.inflight.as_ref().is_some_and(|b| b.wire == Some(Wire::Taken));
            if resent {
                let b = t.inflight.as_mut().unwrap();
                b.seq = t.next_seq;
                t.next_seq += b.msgs.len() as u8;
            }
        }
        out
    });
    assert!(e.as_deref().is_some_and(|e| e.contains("written twice")), "{e:?}");
    let e = first_broken(|s| {
        let mut forgot = s.clone();
        if let Some(b) = &mut forgot.inflight {
            b.maybe_written = false;
        }
        forgot.next(3)
    });
    // a message written then reported failed, whichever invariant sees it first
    assert!(e.as_deref().is_some_and(|e| e.contains("written, reported")), "{e:?}");
}
