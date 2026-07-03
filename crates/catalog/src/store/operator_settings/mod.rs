// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
mod find;
pub(crate) mod shape;

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::row::{JoinTtl, OperatorSettings, Ttl, TtlCleanupMode};
use reifydb_value::value::duration::Duration;

use self::shape::operator_settings;

pub(crate) fn encode_operator_settings(settings: &OperatorSettings) -> EncodedRow {
	let mut row = operator_settings::SHAPE.allocate();

	match &settings.join {
		Some(join) => {
			operator_settings::SHAPE.set_bool(&mut row, operator_settings::IS_JOIN, true);
			encode_side(
				&mut row,
				&join.left,
				operator_settings::LEFT_CLEANUP_MODE,
				operator_settings::LEFT_DURATION,
			);
			encode_side(
				&mut row,
				&join.right,
				operator_settings::RIGHT_CLEANUP_MODE,
				operator_settings::RIGHT_DURATION,
			);
		}
		None => {
			operator_settings::SHAPE.set_bool(&mut row, operator_settings::IS_JOIN, false);
			encode_side(
				&mut row,
				&settings.ttl,
				operator_settings::CLEANUP_MODE,
				operator_settings::DURATION,
			);
		}
	}

	row
}

pub(crate) fn decode_operator_settings(row: &EncodedRow) -> Option<OperatorSettings> {
	if operator_settings::SHAPE.get_bool(row, operator_settings::IS_JOIN) {
		let left = decode_side(row, operator_settings::LEFT_CLEANUP_MODE, operator_settings::LEFT_DURATION)?;
		let right = decode_side(row, operator_settings::RIGHT_CLEANUP_MODE, operator_settings::RIGHT_DURATION)?;
		Some(OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left,
				right,
			}),
		})
	} else {
		let ttl = decode_side(row, operator_settings::CLEANUP_MODE, operator_settings::DURATION)?;
		Some(OperatorSettings {
			ttl,
			join: None,
		})
	}
}

fn encode_side(row: &mut EncodedRow, ttl: &Option<Ttl>, cleanup_idx: usize, duration_idx: usize) {
	match ttl {
		Some(ttl) => {
			operator_settings::SHAPE.set_u8(row, cleanup_idx, encode_cleanup_mode(&ttl.cleanup_mode));
			operator_settings::SHAPE.set_duration(row, duration_idx, ttl.duration);
		}
		None => {
			operator_settings::SHAPE.set_duration(row, duration_idx, Duration::zero());
		}
	}
}

fn decode_side(row: &EncodedRow, cleanup_idx: usize, duration_idx: usize) -> Option<Option<Ttl>> {
	let duration = operator_settings::SHAPE.get_duration(row, duration_idx);
	if duration.is_zero() {
		return Some(None);
	}
	let cleanup_mode = decode_cleanup_mode(operator_settings::SHAPE.get_u8(row, cleanup_idx))?;
	Some(Some(Ttl {
		duration,
		cleanup_mode,
	}))
}

fn encode_cleanup_mode(mode: &TtlCleanupMode) -> u8 {
	match mode {
		TtlCleanupMode::Delete => operator_settings::CLEANUP_MODE_DELETE,
		TtlCleanupMode::Drop => operator_settings::CLEANUP_MODE_DROP,
	}
}

fn decode_cleanup_mode(mode: u8) -> Option<TtlCleanupMode> {
	match mode {
		operator_settings::CLEANUP_MODE_DELETE => Some(TtlCleanupMode::Delete),
		operator_settings::CLEANUP_MODE_DROP => Some(TtlCleanupMode::Drop),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	fn ttl(duration: Duration, cleanup_mode: TtlCleanupMode) -> Ttl {
		Ttl {
			duration,
			cleanup_mode,
		}
	}

	fn roundtrip(settings: OperatorSettings) {
		let encoded = encode_operator_settings(&settings);
		assert_eq!(decode_operator_settings(&encoded).unwrap(), settings);
	}

	#[test]
	fn single_ttl_roundtrips() {
		roundtrip(OperatorSettings {
			ttl: Some(ttl(Duration::from_minutes(5).unwrap(), TtlCleanupMode::Drop)),
			join: None,
		});
		roundtrip(OperatorSettings {
			ttl: Some(ttl(Duration::from_hours(1).unwrap(), TtlCleanupMode::Delete)),
			join: None,
		});
		roundtrip(OperatorSettings {
			ttl: None,
			join: None,
		});
	}

	#[test]
	fn join_ttl_roundtrips_all_side_combinations() {
		let l = ttl(Duration::from_minutes(1).unwrap(), TtlCleanupMode::Drop);
		let r = ttl(Duration::from_minutes(2).unwrap(), TtlCleanupMode::Drop);

		roundtrip(OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(l.clone()),
				right: Some(r.clone()),
			}),
		});
		roundtrip(OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(l),
				right: None,
			}),
		});
		roundtrip(OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: None,
				right: Some(r),
			}),
		});
		roundtrip(OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: None,
				right: None,
			}),
		});
	}
}
