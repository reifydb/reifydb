// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod create;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

use reifydb_core::{
	encoded::row::EncodedRow,
	retention::{CleanupMode, RetentionStrategy},
};

use self::shape::retention_strategy;

/// Encode a RetentionStrategy into EncodedRow
pub(crate) fn encode_retention_strategy(strategy: &RetentionStrategy) -> EncodedRow {
	let mut row = retention_strategy::SHAPE.allocate();

	match strategy {
		RetentionStrategy::KeepForever => {
			retention_strategy::SHAPE.set_u8(
				&mut row,
				retention_strategy::STRATEGY_TYPE,
				retention_strategy::STRATEGY_KEEP_FOREVER,
			);
			// Other fields remain 0/default
		}
		RetentionStrategy::KeepVersions {
			count,
			cleanup_mode,
		} => {
			retention_strategy::SHAPE.set_u8(
				&mut row,
				retention_strategy::STRATEGY_TYPE,
				retention_strategy::STRATEGY_KEEP_VERSIONS,
			);
			retention_strategy::SHAPE.set_u8(
				&mut row,
				retention_strategy::CLEANUP_MODE,
				encode_cleanup_mode(cleanup_mode),
			);
			retention_strategy::SHAPE.set_u64(&mut row, retention_strategy::VALUE, *count);
		}
	}

	row
}

/// Decode a RetentionStrategy from EncodedRow
pub(crate) fn decode_retention_strategy(row: &EncodedRow) -> Option<RetentionStrategy> {
	let strategy_type = retention_strategy::SHAPE.get_u8(row, retention_strategy::STRATEGY_TYPE);

	match strategy_type {
		retention_strategy::STRATEGY_KEEP_FOREVER => Some(RetentionStrategy::KeepForever),

		retention_strategy::STRATEGY_KEEP_VERSIONS => {
			let cleanup_mode = decode_cleanup_mode(
				retention_strategy::SHAPE.get_u8(row, retention_strategy::CLEANUP_MODE),
			)?;
			let count = retention_strategy::SHAPE.get_u64(row, retention_strategy::VALUE);
			Some(RetentionStrategy::KeepVersions {
				count,
				cleanup_mode,
			})
		}

		_ => None,
	}
}

fn encode_cleanup_mode(mode: &CleanupMode) -> u8 {
	match mode {
		CleanupMode::Delete => retention_strategy::CLEANUP_MODE_DELETE,
		CleanupMode::Drop => retention_strategy::CLEANUP_MODE_DROP,
	}
}

fn decode_cleanup_mode(mode: u8) -> Option<CleanupMode> {
	match mode {
		retention_strategy::CLEANUP_MODE_DELETE => Some(CleanupMode::Delete),
		retention_strategy::CLEANUP_MODE_DROP => Some(CleanupMode::Drop),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_keep_forever() {
		let strategy = RetentionStrategy::KeepForever;
		let encoded = encode_retention_strategy(&strategy);
		let decoded = decode_retention_strategy(&encoded).unwrap();
		assert_eq!(strategy, decoded);
	}

	#[test]
	fn test_encode_decode_keep_versions() {
		let strategy = RetentionStrategy::KeepVersions {
			count: 100,
			cleanup_mode: CleanupMode::Delete,
		};
		let encoded = encode_retention_strategy(&strategy);
		let decoded = decode_retention_strategy(&encoded).unwrap();
		assert_eq!(strategy, decoded);
	}
}
