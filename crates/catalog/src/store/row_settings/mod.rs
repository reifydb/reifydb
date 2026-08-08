// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;

pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::row::{RowSettings, Ttl};
use reifydb_value::value::duration::Duration;

use self::shape::row_settings;

pub(crate) fn encode_row_settings(settings: &RowSettings) -> EncodedBytes {
	let mut row = row_settings::SHAPE.allocate();

	match &settings.ttl {
		Some(ttl) => {
			row_settings::SHAPE.set::<bool>(&mut row, row_settings::ANNOUNCE, ttl.announce);
			row_settings::SHAPE.set::<Duration>(&mut row, row_settings::DURATION, ttl.duration);
		}
		None => {
			row_settings::SHAPE.set::<Duration>(&mut row, row_settings::DURATION, Duration::zero());
		}
	}

	row_settings::SHAPE.set::<u8>(&mut row, row_settings::PERSISTENT, u8::from(settings.persistent));

	row.freeze()
}

pub(crate) fn decode_row_settings(bytes: &EncodedBytes) -> Option<RowSettings> {
	let duration = row_settings::SHAPE.get::<Duration>(bytes, row_settings::DURATION);

	let ttl = if duration.is_zero() {
		None
	} else {
		Some(Ttl {
			duration,
			announce: row_settings::SHAPE.get::<bool>(bytes, row_settings::ANNOUNCE),
		})
	};

	let persistent = row_settings::SHAPE.get::<u8>(bytes, row_settings::PERSISTENT) != 0;

	Some(RowSettings {
		ttl,
		persistent,
	})
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_row_settings() {
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
				announce: false,
			}),
			persistent: true,
		};
		let encoded = encode_row_settings(&settings);
		let decoded = decode_row_settings(&encoded).unwrap();
		assert_eq!(settings, decoded);
	}

	#[test]
	fn test_encode_decode_row_settings_announced() {
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_hours(1).unwrap(),
				announce: true,
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
				duration: Duration::from_minutes(1).unwrap(),
				announce: false,
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
