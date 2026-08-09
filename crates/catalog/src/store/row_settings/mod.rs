// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
pub mod create;

pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

use reifydb_core::row::{RowSettings, Ttl};
use reifydb_value::value::duration::Duration;

use self::shape::row_settings;

pub(crate) fn encode_row_settings(settings: &RowSettings) -> EncodedCatalogRow {
	let mut row = row_settings::allocate();

	match &settings.ttl {
		Some(ttl) => {
			row_settings::set_announce(&mut row, ttl.announce);
			row_settings::set_duration(&mut row, ttl.duration);
		}
		None => {
			row_settings::set_duration(&mut row, Duration::zero());
		}
	}

	row_settings::set_persistent(&mut row, u8::from(settings.persistent));

	row.freeze()
}

pub(crate) fn decode_row_settings(bytes: &EncodedCatalogRow) -> Option<RowSettings> {
	let duration = row_settings::get_duration(bytes);

	let ttl = if duration.is_zero() {
		None
	} else {
		Some(Ttl {
			duration,
			announce: row_settings::get_announce(bytes),
		})
	};

	let persistent = row_settings::get_persistent(bytes) != 0;

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
