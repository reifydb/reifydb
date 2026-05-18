// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Small dependency-free utilities used throughout the workspace: the bit-vector backing none-bitmaps and row
//! masks, the copy-on-write vector `CowVec` that the storage tier uses for delta lists, base58/base64/hex codecs,
//! Unicode helpers, and float formatting that respects the workspace's text rendering rules.
//!
//! Everything here has zero ReifyDB-internal dependencies on purpose - so anything in the workspace can pull from
//! it without creating a cycle.

pub mod bitvec;
pub mod cowvec;
pub mod float_format;

pub mod base58;
pub mod base64;
pub mod hex;
pub mod unicode;
