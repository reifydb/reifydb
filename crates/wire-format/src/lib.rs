// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RBCF (ReifyDB Binary Columnar Format) encoder and decoder. The single binary representation that travels between
//! processes - server to client, primary to replica, host to FFI extension - and is independent of any specific
//! transport. The transport crates (gRPC, HTTP, WebSocket, the FFI ABI) all wrap RBCF payloads.
//!
//! The format ships rows as pre-encoded columnar batches alongside a compact schema header so decoders can lay values
//! out directly without round-tripping through a row-shaped intermediate. JSON sits next to it as the human-readable
//! fallback used by tooling and admin endpoints.
//!
//! Invariant: RBCF is wire-stable. Adding a new tag, widening a field, or changing the size or order of an existing
//! field is a coordinated workspace change; old peers must continue to negotiate cleanly. New optional fields go at
//! the end and tolerate absence on the receiving end.

pub mod decode;
pub mod encode;
pub mod encoding;
pub mod error;
pub mod format;
pub mod heuristics;
pub mod json;
pub mod options;
