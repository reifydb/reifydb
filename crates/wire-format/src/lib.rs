// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RBCF (ReifyDB Binary Columnar Format) wire format encoder/decoder.
//!
//! A compact, self-describing binary columnar format for transmitting query results
//! with per-column compression (dictionary, RLE, delta encoding).

pub mod decode;
pub mod encode;
pub mod encoding;
pub mod error;
pub mod format;
pub mod heuristics;
pub mod json;
pub mod options;
