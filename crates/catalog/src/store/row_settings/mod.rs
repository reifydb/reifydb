// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;

pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

use reifydb_core::{
	encoded::row::EncodedRow,
	row::{RowSettings, Ttl, TtlCleanupMode},
};

use self::shape::row_settings;

pub(crate) fn encode_row_settings(settings: &RowSettings) -> EncodedRow {
	let mut row = row_settings::SHAPE.allocate();

	match &settings.ttl {
		Some(ttl) => {
			row_settings::SHAPE.set_u8(
				&mut row,
				row_settings::CLEANUP_MODE,
				encode_cleanup_mode(&ttl.cleanup_mode),
			);
			row_settings::SHAPE.set_u64(&mut row, row_settings::DURATION_NANOS, ttl.duration_nanos);
		}
		None => {
			row_settings::SHAPE.set_u64(&mut row, row_settings::DURATION_NANOS, 0u64);
		}
	}

	row_settings::SHAPE.set_u8(&mut row, row_settings::PERSISTENT, u8::from(settings.persistent));

	row
}

pub(crate) fn decode_row_settings(row: &EncodedRow) -> Option<RowSettings> {
	let duration_nanos = row_settings::SHAPE.get_u64(row, row_settings::DURATION_NANOS);

	let ttl = if duration_nanos == 0 {
		None
	} else {
		let cleanup_mode = decode_cleanup_mode(row_settings::SHAPE.get_u8(row, row_settings::CLEANUP_MODE))?;
		Some(Ttl {
			duration_nanos,
			cleanup_mode,
		})
	};

	let persistent = row_settings::SHAPE.get_u8(row, row_settings::PERSISTENT) != 0;

	Some(RowSettings {
		ttl,
		persistent,
	})
}

fn encode_cleanup_mode(mode: &TtlCleanupMode) -> u8 {
	match mode {
		TtlCleanupMode::Delete => row_settings::CLEANUP_MODE_DELETE,
		TtlCleanupMode::Drop => row_settings::CLEANUP_MODE_DROP,
	}
}

fn decode_cleanup_mode(mode: u8) -> Option<TtlCleanupMode> {
	match mode {
		row_settings::CLEANUP_MODE_DELETE => Some(TtlCleanupMode::Delete),
		row_settings::CLEANUP_MODE_DROP => Some(TtlCleanupMode::Drop),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_row_settings() {
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 300_000_000_000, // 5 minutes
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};
		let encoded = encode_row_settings(&settings);
		let decoded = decode_row_settings(&encoded).unwrap();
		assert_eq!(settings, decoded);
	}

	#[test]
	fn test_encode_decode_row_settings_updated_delete() {
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 3_600_000_000_000, // 1 hour
				cleanup_mode: TtlCleanupMode::Delete,
			}),
			persistent: true,
		};
		let encoded = encode_row_settings(&settings);
		let decoded = decode_row_settings(&encoded).unwrap();
		assert_eq!(settings, decoded);
	}

	#[test]
	fn test_encode_decode_row_settings_non_persistent() {
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 60_000_000_000,
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: false,
		};
		let encoded = encode_row_settings(&settings);
		let decoded = decode_row_settings(&encoded).unwrap();
		assert_eq!(settings, decoded);
		assert!(!decoded.persistent);
	}

	#[test]
	fn test_encode_decode_row_settings_no_ttl() {
		let settings = RowSettings {
			ttl: None,
			persistent: false,
		};
		let encoded = encode_row_settings(&settings);
		let decoded = decode_row_settings(&encoded).unwrap();
		assert_eq!(decoded.ttl, None);
		assert!(!decoded.persistent);
	}
}
