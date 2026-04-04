// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod create;

pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

use reifydb_core::{
	encoded::row::EncodedRow,
	row::{RowTtl, RowTtlAnchor, RowTtlCleanupMode},
};

use self::shape::ttl_config;

/// Encode a RowTtl into EncodedRow
pub(crate) fn encode_ttl_config(config: &RowTtl) -> EncodedRow {
	let mut row = ttl_config::SHAPE.allocate();

	ttl_config::SHAPE.set_u8(&mut row, ttl_config::ANCHOR, encode_anchor(&config.anchor));
	ttl_config::SHAPE.set_u8(&mut row, ttl_config::CLEANUP_MODE, encode_cleanup_mode(&config.cleanup_mode));
	ttl_config::SHAPE.set_u64(&mut row, ttl_config::DURATION_NANOS, config.duration_nanos);

	row
}

/// Decode a RowTtl from EncodedRow
pub(crate) fn decode_ttl_config(row: &EncodedRow) -> Option<RowTtl> {
	let anchor = decode_anchor(ttl_config::SHAPE.get_u8(row, ttl_config::ANCHOR))?;
	let cleanup_mode = decode_cleanup_mode(ttl_config::SHAPE.get_u8(row, ttl_config::CLEANUP_MODE))?;
	let duration_nanos = ttl_config::SHAPE.get_u64(row, ttl_config::DURATION_NANOS);

	Some(RowTtl {
		duration_nanos,
		anchor,
		cleanup_mode,
	})
}

fn encode_anchor(anchor: &RowTtlAnchor) -> u8 {
	match anchor {
		RowTtlAnchor::Created => ttl_config::ANCHOR_CREATED,
		RowTtlAnchor::Updated => ttl_config::ANCHOR_UPDATED,
	}
}

fn decode_anchor(anchor: u8) -> Option<RowTtlAnchor> {
	match anchor {
		ttl_config::ANCHOR_CREATED => Some(RowTtlAnchor::Created),
		ttl_config::ANCHOR_UPDATED => Some(RowTtlAnchor::Updated),
		_ => None,
	}
}

fn encode_cleanup_mode(mode: &RowTtlCleanupMode) -> u8 {
	match mode {
		RowTtlCleanupMode::Delete => ttl_config::CLEANUP_MODE_DELETE,
		RowTtlCleanupMode::Drop => ttl_config::CLEANUP_MODE_DROP,
	}
}

fn decode_cleanup_mode(mode: u8) -> Option<RowTtlCleanupMode> {
	match mode {
		ttl_config::CLEANUP_MODE_DELETE => Some(RowTtlCleanupMode::Delete),
		ttl_config::CLEANUP_MODE_DROP => Some(RowTtlCleanupMode::Drop),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_ttl_config() {
		let config = RowTtl {
			duration_nanos: 300_000_000_000, // 5 minutes
			anchor: RowTtlAnchor::Created,
			cleanup_mode: RowTtlCleanupMode::Drop,
		};
		let encoded = encode_ttl_config(&config);
		let decoded = decode_ttl_config(&encoded).unwrap();
		assert_eq!(config, decoded);
	}

	#[test]
	fn test_encode_decode_ttl_config_updated_delete() {
		let config = RowTtl {
			duration_nanos: 3_600_000_000_000, // 1 hour
			anchor: RowTtlAnchor::Updated,
			cleanup_mode: RowTtlCleanupMode::Delete,
		};
		let encoded = encode_ttl_config(&config);
		let decoded = decode_ttl_config(&encoded).unwrap();
		assert_eq!(config, decoded);
	}
}
