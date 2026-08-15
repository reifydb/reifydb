// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
mod find;
pub(crate) mod shape;

use reifydb_codec::row::catalog::{EncodedCatalogRow, EncodedCatalogRowBuilder};
use reifydb_core::row::{JoinLateness, OperatorLateness, OperatorSettings};
use reifydb_value::value::duration::Duration;

use self::shape::operator_settings;

pub(crate) fn encode_operator_settings(settings: &OperatorSettings) -> EncodedCatalogRow {
	let mut row = operator_settings::allocate();

	match &settings.join {
		Some(join) => {
			operator_settings::set_is_join(&mut row, true);
			encode_side(&mut row, &join.left, operator_settings::LEFT_DURATION);
			encode_side(&mut row, &join.right, operator_settings::RIGHT_DURATION);
		}
		None => {
			operator_settings::set_is_join(&mut row, false);
			encode_side(&mut row, &settings.lateness, operator_settings::DURATION);
		}
	}

	row.freeze()
}

pub(crate) fn decode_operator_settings(bytes: &EncodedCatalogRow) -> Option<OperatorSettings> {
	if operator_settings::get_is_join(bytes) {
		let left = decode_side(bytes, operator_settings::LEFT_DURATION);
		let right = decode_side(bytes, operator_settings::RIGHT_DURATION);
		Some(OperatorSettings {
			lateness: None,
			join: Some(JoinLateness {
				left,
				right,
			}),
		})
	} else {
		Some(OperatorSettings {
			lateness: decode_side(bytes, operator_settings::DURATION),
			join: None,
		})
	}
}

fn encode_side(row: &mut EncodedCatalogRowBuilder, lateness: &Option<OperatorLateness>, duration_idx: usize) {
	let duration = lateness.as_ref().map(|lateness| lateness.duration).unwrap_or_else(Duration::zero);
	operator_settings::SHAPE.set::<Duration>(row, duration_idx, duration);
}

fn decode_side(bytes: &EncodedCatalogRow, duration_idx: usize) -> Option<OperatorLateness> {
	let duration = operator_settings::SHAPE.get::<Duration>(bytes.as_slice(), duration_idx);
	if duration.is_zero() {
		return None;
	}
	Some(OperatorLateness {
		duration,
	})
}

#[cfg(test)]
pub mod tests {
	use super::*;

	fn ttl(duration: Duration) -> OperatorLateness {
		OperatorLateness {
			duration,
		}
	}

	fn roundtrip(settings: OperatorSettings) {
		let encoded = encode_operator_settings(&settings);
		assert_eq!(decode_operator_settings(&encoded).unwrap(), settings);
	}

	#[test]
	fn single_ttl_roundtrips() {
		roundtrip(OperatorSettings {
			lateness: Some(ttl(Duration::from_minutes(5).unwrap())),
			join: None,
		});
		roundtrip(OperatorSettings {
			lateness: Some(ttl(Duration::from_hours(1).unwrap())),
			join: None,
		});
		roundtrip(OperatorSettings {
			lateness: None,
			join: None,
		});
	}

	#[test]
	fn join_ttl_roundtrips_all_side_combinations() {
		let l = ttl(Duration::from_minutes(1).unwrap());
		let r = ttl(Duration::from_minutes(2).unwrap());

		roundtrip(OperatorSettings {
			lateness: None,
			join: Some(JoinLateness {
				left: Some(l.clone()),
				right: Some(r.clone()),
			}),
		});
		roundtrip(OperatorSettings {
			lateness: None,
			join: Some(JoinLateness {
				left: Some(l),
				right: None,
			}),
		});
		roundtrip(OperatorSettings {
			lateness: None,
			join: Some(JoinLateness {
				left: None,
				right: Some(r),
			}),
		});
		roundtrip(OperatorSettings {
			lateness: None,
			join: Some(JoinLateness {
				left: None,
				right: None,
			}),
		});
	}

	#[test]
	fn operator_settings_shape_carries_no_announce_axis() {
		// Operator state is structurally excluded from CDC, so an operator TTL has no announce
		// axis; a mode column reappearing in the shape would mean it leaked back in.
		let names: Vec<&str> = operator_settings::SHAPE.fields().iter().map(|f| f.name.as_str()).collect();
		assert_eq!(names, vec!["is_join", "duration", "left_duration", "right_duration"]);
	}
}
