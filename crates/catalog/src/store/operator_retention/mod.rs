// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub(crate) mod shape;

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::row::OperatorRetention;
use reifydb_value::value::duration::Duration;

use self::shape::operator_retention;

pub(crate) fn encode_operator_retention(retention: &OperatorRetention) -> EncodedCatalogRow {
	let mut row = operator_retention::allocate();
	operator_retention::SHAPE.set::<Duration>(&mut row, operator_retention::DURATION, retention.duration);
	row.freeze()
}

pub(crate) fn decode_operator_retention(bytes: &EncodedCatalogRow) -> Option<OperatorRetention> {
	Some(OperatorRetention {
		duration: operator_retention::SHAPE.get::<Duration>(bytes.as_slice(), operator_retention::DURATION),
	})
}

#[cfg(test)]
pub mod tests {
	use super::*;

	fn roundtrip(retention: OperatorRetention) {
		let encoded = encode_operator_retention(&retention);
		assert_eq!(decode_operator_retention(&encoded).unwrap(), retention);
	}

	#[test]
	fn retention_roundtrips() {
		roundtrip(OperatorRetention {
			duration: Duration::from_minutes(5).unwrap(),
		});
		roundtrip(OperatorRetention {
			duration: Duration::from_hours(1).unwrap(),
		});
	}

	#[test]
	fn operator_retention_shape_carries_no_announce_axis() {
		// Operator state is structurally excluded from CDC, so an operator TTL has no announce
		// axis; a mode column reappearing in the shape would mean it leaked back in.
		let names: Vec<&str> = operator_retention::SHAPE.fields().iter().map(|f| f.name.as_str()).collect();
		assert_eq!(names, vec!["duration"]);
	}
}
