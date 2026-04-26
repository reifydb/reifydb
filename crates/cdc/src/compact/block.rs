// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! On-disk format for compacted CDC blocks: zstd(postcard(Vec<Cdc>)).
//! Entries inside a block are sorted ascending by Cdc.version. The block's
//! min/max version bounds are stored as separate columns in cdc_block so we
//! never decompress to answer range queries.

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use zstd::{decode_all, encode_all};

use crate::error::CdcError;

#[derive(Debug, Clone)]
pub struct CompactBlockSummary {
	pub min_version: CommitVersion,
	pub max_version: CommitVersion,
	pub num_entries: usize,
	pub compressed_bytes: usize,
}

pub fn encode(entries: &[Cdc], zstd_level: u8) -> Result<Vec<u8>, CdcError> {
	debug_assert!(!entries.is_empty(), "cannot encode an empty block");
	debug_assert!(
		entries.windows(2).all(|w| w[0].version < w[1].version),
		"block entries must be strictly ascending by version"
	);
	let raw = to_stdvec(entries).map_err(|e| CdcError::Codec(format!("postcard encode block: {e}")))?;
	let compressed = encode_all(&raw[..], zstd_level as i32)
		.map_err(|e| CdcError::Codec(format!("zstd encode block: {e}")))?;
	Ok(compressed)
}

pub fn decode(bytes: &[u8]) -> Result<Vec<Cdc>, CdcError> {
	let raw = decode_all(bytes).map_err(|e| CdcError::Codec(format!("zstd decode block: {e}")))?;
	let entries: Vec<Cdc> = from_bytes(&raw).map_err(|e| CdcError::Codec(format!("postcard decode block: {e}")))?;
	Ok(entries)
}
