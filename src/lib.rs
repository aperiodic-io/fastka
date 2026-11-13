#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), warn(missing_docs))]
pub mod client;
pub mod config;
pub(crate) mod conn;
pub mod consumer;
pub mod error;
pub mod producer;
#[cfg(kani)]
mod proofs;
pub mod records;
pub(crate) mod sasl;
#[cfg(test)]
mod testkit;
mod tls;
/// The protocol's requests and responses. Not part of the stable API: public for the fuzzers and
/// the benchmark gate only.
#[doc(hidden)]
pub mod wire;

pub use config::Config;
pub use error::{Code, Error, Result};
