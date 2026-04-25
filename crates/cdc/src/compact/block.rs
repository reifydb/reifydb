// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! On-disk format for compacted CDC blocks: zstd(postcard(Vec<Cdc>)).
//! Entries inside a block are sorted ascending by Cdc.version. The block's
//! min/max version bounds are stored as separate columns in cdc_block so we
//! never decompress to answer range queries.

use postcard::{from_bytes, to_stdvec};
use reifydb_core::interface::cdc::Cdc;
use zstd::{decode_all, encode_all};

use crate::error::CdcError;

const ZSTD_LEVEL: i32 = 3;

pub fn encode(entries: &[Cdc]) -> Result<Vec<u8>, CdcError> {
	debug_assert!(!entries.is_empty(), "cannot encode an empty block");
	debug_assert!(
		entries.windows(2).all(|w| w[0].version < w[1].version),
		"block entries must be strictly ascending by version"
	);
	let raw = to_stdvec(entries).map_err(|e| CdcError::Codec(format!("postcard encode block: {e}")))?;
	let compressed =
		encode_all(&raw[..], ZSTD_LEVEL).map_err(|e| CdcError::Codec(format!("zstd encode block: {e}")))?;
	Ok(compressed)
}

pub fn decode(bytes: &[u8]) -> Result<Vec<Cdc>, CdcError> {
	let raw = decode_all(bytes).map_err(|e| CdcError::Codec(format!("zstd decode block: {e}")))?;
	let entries: Vec<Cdc> = from_bytes(&raw).map_err(|e| CdcError::Codec(format!("postcard decode block: {e}")))?;
	Ok(entries)
}
