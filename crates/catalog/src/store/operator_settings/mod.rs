// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod create;
pub(crate) mod shape;

use reifydb_core::{
	encoded::row::EncodedRow,
	row::{JoinTtl, OperatorSettings, Ttl, TtlAnchor, TtlCleanupMode},
};

use self::shape::operator_settings;

pub(crate) fn encode_operator_settings(settings: &OperatorSettings) -> EncodedRow {
	let mut row = operator_settings::SHAPE.allocate();

	match &settings.join {
		Some(join) => {
			operator_settings::SHAPE.set_bool(&mut row, operator_settings::IS_JOIN, true);
			encode_side(
				&mut row,
				&join.left,
				operator_settings::LEFT_ANCHOR,
				operator_settings::LEFT_CLEANUP_MODE,
				operator_settings::LEFT_DURATION_NANOS,
			);
			encode_side(
				&mut row,
				&join.right,
				operator_settings::RIGHT_ANCHOR,
				operator_settings::RIGHT_CLEANUP_MODE,
				operator_settings::RIGHT_DURATION_NANOS,
			);
		}
		None => {
			operator_settings::SHAPE.set_bool(&mut row, operator_settings::IS_JOIN, false);
			encode_side(
				&mut row,
				&settings.ttl,
				operator_settings::ANCHOR,
				operator_settings::CLEANUP_MODE,
				operator_settings::DURATION_NANOS,
			);
		}
	}

	row
}

pub(crate) fn decode_operator_settings(row: &EncodedRow) -> Option<OperatorSettings> {
	if operator_settings::SHAPE.get_bool(row, operator_settings::IS_JOIN) {
		let left = decode_side(
			row,
			operator_settings::LEFT_ANCHOR,
			operator_settings::LEFT_CLEANUP_MODE,
			operator_settings::LEFT_DURATION_NANOS,
		)?;
		let right = decode_side(
			row,
			operator_settings::RIGHT_ANCHOR,
			operator_settings::RIGHT_CLEANUP_MODE,
			operator_settings::RIGHT_DURATION_NANOS,
		)?;
		Some(OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left,
				right,
			}),
		})
	} else {
		let ttl = decode_side(
			row,
			operator_settings::ANCHOR,
			operator_settings::CLEANUP_MODE,
			operator_settings::DURATION_NANOS,
		)?;
		Some(OperatorSettings {
			ttl,
			join: None,
		})
	}
}

fn encode_side(row: &mut EncodedRow, ttl: &Option<Ttl>, anchor_idx: usize, cleanup_idx: usize, duration_idx: usize) {
	match ttl {
		Some(ttl) => {
			operator_settings::SHAPE.set_u8(row, anchor_idx, encode_anchor(&ttl.anchor));
			operator_settings::SHAPE.set_u8(row, cleanup_idx, encode_cleanup_mode(&ttl.cleanup_mode));
			operator_settings::SHAPE.set_u64(row, duration_idx, ttl.duration_nanos);
		}
		None => {
			operator_settings::SHAPE.set_u64(row, duration_idx, 0u64);
		}
	}
}

fn decode_side(row: &EncodedRow, anchor_idx: usize, cleanup_idx: usize, duration_idx: usize) -> Option<Option<Ttl>> {
	let duration_nanos = operator_settings::SHAPE.get_u64(row, duration_idx);
	if duration_nanos == 0 {
		return Some(None);
	}
	let anchor = decode_anchor(operator_settings::SHAPE.get_u8(row, anchor_idx))?;
	let cleanup_mode = decode_cleanup_mode(operator_settings::SHAPE.get_u8(row, cleanup_idx))?;
	Some(Some(Ttl {
		duration_nanos,
		anchor,
		cleanup_mode,
	}))
}

fn encode_anchor(anchor: &TtlAnchor) -> u8 {
	match anchor {
		TtlAnchor::Created => operator_settings::ANCHOR_CREATED,
		TtlAnchor::Updated => operator_settings::ANCHOR_UPDATED,
	}
}

fn decode_anchor(anchor: u8) -> Option<TtlAnchor> {
	match anchor {
		operator_settings::ANCHOR_CREATED => Some(TtlAnchor::Created),
		operator_settings::ANCHOR_UPDATED => Some(TtlAnchor::Updated),
		_ => None,
	}
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

	fn ttl(duration_nanos: u64, anchor: TtlAnchor, cleanup_mode: TtlCleanupMode) -> Ttl {
		Ttl {
			duration_nanos,
			anchor,
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
			ttl: Some(ttl(300_000_000_000, TtlAnchor::Created, TtlCleanupMode::Drop)),
			join: None,
		});
		roundtrip(OperatorSettings {
			ttl: Some(ttl(3_600_000_000_000, TtlAnchor::Updated, TtlCleanupMode::Delete)),
			join: None,
		});
		roundtrip(OperatorSettings {
			ttl: None,
			join: None,
		});
	}

	#[test]
	fn join_ttl_roundtrips_all_side_combinations() {
		let l = ttl(60_000_000_000, TtlAnchor::Updated, TtlCleanupMode::Drop);
		let r = ttl(120_000_000_000, TtlAnchor::Updated, TtlCleanupMode::Drop);

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
