#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
//! Core SCPI protocol and connection layer for QSlib.
//!
//! This crate holds the transport-independent pieces of QSlib: the SCPI
//! message parser ([`parser`]), the incremental message framer
//! ([`message_receiver`]), the command/response types ([`commands`]), and the
//! async connection ([`com`]). It carries no Polars, no EDS/quant/calibration
//! parsing, and — unless the `python` or `tls` features are enabled — no pyo3
//! and no rustls, so it links cleanly into a plain binary (e.g. `qslib-server`)
//! and cross-compiles to static musl.
//!
//! Higher-level, domain-specific helpers (plate setup, filter data, protocol
//! parsing) live in the `qslib` crate, which depends on this one.

pub mod com;
pub mod commands;
pub mod message_receiver;
pub mod parser;
