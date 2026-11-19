// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub(crate) mod shape;

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::row::OperatorHorizon;
use reifydb_value::value::duration::Duration;

use self::shape::operator_horizon;

pub(crate) fn encode_operator_horizon(horizon: &OperatorHorizon) -> EncodedCatalogRow {
	let mut row = operator_horizon::allocate();
	operator_horizon::SHAPE.set::<Duration>(&mut row, operator_horizon::DURATION, horizon.duration);
	row.freeze()
}

pub(crate) fn decode_operator_horizon(bytes: &EncodedCatalogRow) -> Option<OperatorHorizon> {
	Some(OperatorHorizon {
		duration: operator_horizon::SHAPE.get::<Duration>(bytes.as_slice(), operator_horizon::DURATION),
	})
}

#[cfg(test)]
pub mod tests {
	use super::*;

	fn roundtrip(horizon: OperatorHorizon) {
		let encoded = encode_operator_horizon(&horizon);
		assert_eq!(decode_operator_horizon(&encoded).unwrap(), horizon);
	}

	#[test]
	fn horizon_roundtrips() {
		roundtrip(OperatorHorizon {
			duration: Duration::from_minutes(5).unwrap(),
		});
		roundtrip(OperatorHorizon {
			duration: Duration::from_hours(1).unwrap(),
		});
	}

	#[test]
	fn operator_horizon_shape_carries_no_announce_axis() {
		// Operator state is structurally excluded from commit log, so an operator TTL has no announce
		// axis; a mode column reappearing in the shape would mean it leaked back in.
		let names: Vec<&str> = operator_horizon::SHAPE.fields().iter().map(|f| f.name.as_str()).collect();
		assert_eq!(names, vec!["duration"]);
	}
}
