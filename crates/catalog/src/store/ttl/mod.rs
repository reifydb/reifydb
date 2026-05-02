// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod create;

pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

use reifydb_core::{
	encoded::row::EncodedRow,
	row::{Ttl, TtlAnchor, TtlCleanupMode},
};

use self::shape::ttl_config;

/// Encode a Ttl into EncodedRow
pub(crate) fn encode_ttl_config(config: &Ttl) -> EncodedRow {
	let mut row = ttl_config::SHAPE.allocate();

	ttl_config::SHAPE.set_u8(&mut row, ttl_config::ANCHOR, encode_anchor(&config.anchor));
	ttl_config::SHAPE.set_u8(&mut row, ttl_config::CLEANUP_MODE, encode_cleanup_mode(&config.cleanup_mode));
	ttl_config::SHAPE.set_u64(&mut row, ttl_config::DURATION_NANOS, config.duration_nanos);

	row
}

/// Decode a Ttl from EncodedRow
pub(crate) fn decode_ttl_config(row: &EncodedRow) -> Option<Ttl> {
	let anchor = decode_anchor(ttl_config::SHAPE.get_u8(row, ttl_config::ANCHOR))?;
	let cleanup_mode = decode_cleanup_mode(ttl_config::SHAPE.get_u8(row, ttl_config::CLEANUP_MODE))?;
	let duration_nanos = ttl_config::SHAPE.get_u64(row, ttl_config::DURATION_NANOS);

	Some(Ttl {
		duration_nanos,
		anchor,
		cleanup_mode,
	})
}

fn encode_anchor(anchor: &TtlAnchor) -> u8 {
	match anchor {
		TtlAnchor::Created => ttl_config::ANCHOR_CREATED,
		TtlAnchor::Updated => ttl_config::ANCHOR_UPDATED,
	}
}

fn decode_anchor(anchor: u8) -> Option<TtlAnchor> {
	match anchor {
		ttl_config::ANCHOR_CREATED => Some(TtlAnchor::Created),
		ttl_config::ANCHOR_UPDATED => Some(TtlAnchor::Updated),
		_ => None,
	}
}

fn encode_cleanup_mode(mode: &TtlCleanupMode) -> u8 {
	match mode {
		TtlCleanupMode::Delete => ttl_config::CLEANUP_MODE_DELETE,
		TtlCleanupMode::Drop => ttl_config::CLEANUP_MODE_DROP,
	}
}

fn decode_cleanup_mode(mode: u8) -> Option<TtlCleanupMode> {
	match mode {
		ttl_config::CLEANUP_MODE_DELETE => Some(TtlCleanupMode::Delete),
		ttl_config::CLEANUP_MODE_DROP => Some(TtlCleanupMode::Drop),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_ttl_config() {
		let config = Ttl {
			duration_nanos: 300_000_000_000, // 5 minutes
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let encoded = encode_ttl_config(&config);
		let decoded = decode_ttl_config(&encoded).unwrap();
		assert_eq!(config, decoded);
	}

	#[test]
	fn test_encode_decode_ttl_config_updated_delete() {
		let config = Ttl {
			duration_nanos: 3_600_000_000_000, // 1 hour
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Delete,
		};
		let encoded = encode_ttl_config(&config);
		let decoded = decode_ttl_config(&encoded).unwrap();
		assert_eq!(config, decoded);
	}
}
