// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Per-column wire encodings: plain, dictionary, run-length, and delta. Each encoding has a stable tag in the RBCF
//! header so the decoder can pick the right reader without out-of-band coordination. New encodings need a new tag
//! and a coordinated update on every peer that might receive the resulting payload.

pub mod delta;
pub mod dict;
pub mod plain;
pub mod rle;
