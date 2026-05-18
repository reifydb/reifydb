// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Decoders for the encoded-row layouts catalog shapes use. Each shape (table, ringbuffer, series, dictionary)
//! has a different on-disk record format; this module owns the byte-level decoding so the rest of the catalog
//! code can work with typed values instead of raw bytes.

pub mod decode;
