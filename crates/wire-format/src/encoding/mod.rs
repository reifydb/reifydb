// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Per-column encoding implementations (Plain, Dict, RLE, Delta, DeltaRLE).

pub mod delta;
pub mod dict;
pub mod plain;
pub mod rle;
