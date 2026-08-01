// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Byte-level decoders for the encoded-row layouts catalog objects use, one per on-disk record
//! format, so the rest of the catalog works with typed values instead of raw bytes.

pub mod decode;
