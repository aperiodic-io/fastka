//! A synchronous Kafka client for brrrrr (ADR-0029).
pub mod client;
pub mod config;
pub mod conn;
pub mod consumer;
pub mod error;
pub mod producer;
pub mod records;
pub mod sasl;
#[cfg(test)]
mod testkit;
#[cfg(kani)]
mod proofs;
mod tls;
pub mod wire;

pub use config::Config;
pub use error::{Code, Error, Result};
