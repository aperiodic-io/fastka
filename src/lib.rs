//! A synchronous Kafka client.
pub mod client;
pub mod config;
pub(crate) mod conn;
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
