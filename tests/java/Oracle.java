// The acceptance tests' reference: Apache Kafka's own Java client, doing what fastka is checked
// against. Each command prints one JSON line per record (or a result) on stdout.
//
//   produce   <bootstrap> <topic> <codec> <n>             keyed (k<i%50>), headers, a tombstone each 10th
//   produce-txn <bootstrap> <topic> <txns> <per>          transactions, every 3rd aborted
//   produce-open <bootstrap> <topic> <n> <hold-ms>        one transaction held open, then committed
//   consume   <bootstrap> <topic> <partitions> <n> <isolation>
//   quota     <bootstrap> <client-id> <bytes-per-s>        producer and consumer byte rates
//   group-offsets <bootstrap> <group>
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.quota.*;
import org.apache.kafka.common.serialization.*;

public class Oracle {
    static String hex(byte[] b) {
        if (b == null) return "null";
        StringBuilder s = new StringBuilder("\"");
        for (byte x : b) s.append(String.format("%02x", x));
        return s.append('"').toString();
    }

    static Properties props(String bootstrap) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("client.id", "fastka-oracle");
        return p;
    }

    static KafkaProducer<byte[], byte[]> producer(Properties p) {
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        return new KafkaProducer<>(p);
    }

    public static void main(String[] a) throws Exception {
        switch (a[0]) {
            case "produce": {
                Properties p = props(a[1]);
                p.put("compression.type", a[3]);
                p.put("linger.ms", "5");
                try (KafkaProducer<byte[], byte[]> pr = producer(p)) {
                    int n = Integer.parseInt(a[4]);
                    List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        byte[] k = ("k" + (i % 50)).getBytes(StandardCharsets.UTF_8);
                        byte[] v = i % 10 == 9 ? null : ("v" + i + "-" + "x".repeat(i % 31)).getBytes(StandardCharsets.UTF_8);
                        ProducerRecord<byte[], byte[]> r = new ProducerRecord<>(a[2], null, 1_700_000_000_000L + i, k, v);
                        r.headers().add("i", Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                        r.headers().add("nil", null);
                        fs.add(pr.send(r));
                    }
                    for (int i = 0; i < n; i++) {
                        RecordMetadata m = fs.get(i).get();
                        System.out.println("{\"i\":" + i + ",\"partition\":" + m.partition() + ",\"offset\":" + m.offset() + "}");
                    }
                }
                break;
            }
            case "produce-txn": {
                Properties p = props(a[1]);
                p.put("transactional.id", "fastka-oracle-" + UUID.randomUUID());
                try (KafkaProducer<byte[], byte[]> pr = producer(p)) {
                    pr.initTransactions();
                    int txns = Integer.parseInt(a[3]), per = Integer.parseInt(a[4]);
                    for (int t = 0; t < txns; t++) {
                        boolean abort = t % 3 == 2;
                        pr.beginTransaction();
                        for (int i = 0; i < per; i++) {
                            String v = (abort ? "a" : "c") + t + "-" + i;
                            pr.send(new ProducerRecord<>(a[2], i % 2, null, v.getBytes(StandardCharsets.UTF_8)));
                        }
                        // sent before aborting: the producer drops what it has not sent yet, and an
                        // aborted transaction must be on the log for read_committed to skip it
                        pr.flush();
                        if (abort) pr.abortTransaction(); else pr.commitTransaction();
                    }
                }
                System.out.println("{\"done\":true}");
                break;
            }
            case "produce-open": {
                Properties p = props(a[1]);
                p.put("transactional.id", "fastka-oracle-open-" + UUID.randomUUID());
                try (KafkaProducer<byte[], byte[]> pr = producer(p)) {
                    pr.initTransactions();
                    pr.beginTransaction();
                    for (int i = 0; i < Integer.parseInt(a[3]); i++)
                        pr.send(new ProducerRecord<>(a[2], 0, null, ("open-" + i).getBytes(StandardCharsets.UTF_8)));
                    pr.flush();
                    System.out.println("{\"open\":true}");
                    System.out.flush();
                    Thread.sleep(Long.parseLong(a[4]));
                    pr.commitTransaction();
                }
                System.out.println("{\"committed\":true}");
                break;
            }
            case "consume": {
                Properties p = props(a[1]);
                p.put("key.deserializer", ByteArrayDeserializer.class.getName());
                p.put("value.deserializer", ByteArrayDeserializer.class.getName());
                p.put("isolation.level", a[5]);
                p.put("enable.auto.commit", "false");
                try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
                    List<TopicPartition> tps = new ArrayList<>();
                    for (int i = 0; i < Integer.parseInt(a[3]); i++) tps.add(new TopicPartition(a[2], i));
                    c.assign(tps);
                    c.seekToBeginning(tps);
                    int n = Integer.parseInt(a[4]), got = 0;
                    long until = System.currentTimeMillis() + 60_000;
                    while (got < n && System.currentTimeMillis() < until) {
                        for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(200))) {
                            StringBuilder h = new StringBuilder("[");
                            for (Header x : r.headers()) {
                                if (h.length() > 1) h.append(',');
                                h.append("[\"").append(x.key()).append("\",").append(hex(x.value())).append(']');
                            }
                            System.out.println("{\"partition\":" + r.partition() + ",\"offset\":" + r.offset()
                                + ",\"timestamp\":" + r.timestamp() + ",\"key\":" + hex(r.key()) + ",\"value\":"
                                + hex(r.value()) + ",\"headers\":" + h.append(']') + "}");
                            got++;
                        }
                    }
                }
                break;
            }
            case "quota": {
                try (Admin ad = Admin.create(props(a[1]))) {
                    ClientQuotaEntity e = new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, a[2]));
                    double rate = Double.parseDouble(a[3]);
                    ad.alterClientQuotas(List.of(new ClientQuotaAlteration(e, List.of(
                        new ClientQuotaAlteration.Op("producer_byte_rate", rate),
                        new ClientQuotaAlteration.Op("consumer_byte_rate", rate))))).all().get();
                }
                System.out.println("{\"quota\":true}");
                break;
            }
            case "group-offsets": {
                try (Admin ad = Admin.create(props(a[1]))) {
                    var m = ad.listConsumerGroupOffsets(a[2]).partitionsToOffsetAndMetadata().get();
                    for (var e : new TreeMap<String, Long>(m.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                            x -> x.getKey().topic() + "/" + x.getKey().partition(), x -> x.getValue().offset()))).entrySet())
                        System.out.println("{\"tp\":\"" + e.getKey() + "\",\"offset\":" + e.getValue() + "}");
                }
                break;
            }
            default:
                throw new IllegalArgumentException(a[0]);
        }
    }
}
