// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Small utilities: the bit-vector behind none-bitmaps and row masks, the copy-on-write `CowVec`,
//! base58/base64/hex codecs, Unicode helpers and float formatting. Everything here has zero
//! ReifyDB-internal dependencies on purpose, so any crate can pull from it without a cycle.

pub mod bitvec;
pub mod cowvec;
pub mod float_format;

pub mod base58;
pub mod base64;
pub mod hash;
pub mod hex;
pub mod unicode;
