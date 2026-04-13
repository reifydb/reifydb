// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! JSON wire format for frames.
//!
//! Single canonical implementation of `Frame` ↔ JSON conversion shared by:
//! - The server's HTTP/WS JSON responses (via `convert_frames`).
//! - Rust and TypeScript clients deserializing those responses.
//! - The `rbcf::encode` / `rbcf::decode` RQL routines.
//!
//! JSON shape:
//!
//! ```json
//! [{
//!   "row_numbers": ["1","2"],
//!   "created_at": ["..."],
//!   "updated_at": ["..."],
//!   "columns": [{ "name": "b", "type": "Blob", "payload": ["0xdeadbeef"] }]
//! }]
//! ```
//!
//! Nones are represented with the `"⟪none⟫"` sentinel string in `payload`.

pub mod from;
pub mod to;
pub mod types;

pub use from::{convert_column_to_data, convert_envelope_response, frames_from_json};
pub use to::{convert_frames, frames_to_json};
pub use types::{ResponseColumn, ResponseFrame};
