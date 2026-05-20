// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Decoders for the encoded-row layouts catalog shapes use. Each shape (table, ringbuffer, series, dictionary)
//! has a different on-disk record format; this module owns the byte-level decoding so the rest of the catalog
//! code can work with typed values instead of raw bytes.

pub mod decode;
