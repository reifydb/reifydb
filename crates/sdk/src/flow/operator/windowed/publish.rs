// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::{Value, datetime::DateTime, row_number::RowNumber};

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		column::{row::Row, sink::in_process::InProcessRowSink},
		view::in_process::InProcessRowView,
	},
};

pub fn row_to_values<R: Row>(row: &R) -> Result<Vec<Value>> {
	let mut sink = InProcessRowSink::new(R::COLUMNS)?;
	row.encode_into(&mut sink)?;
	let columns = sink.finish(vec![RowNumber(0)], DateTime::default())?;
	Ok(columns.row(0))
}

pub fn values_to_row<R: Row>(values: &[Value]) -> Result<R> {
	let names: Vec<&str> = R::COLUMNS.iter().map(|(name, _)| *name).collect();
	if values.len() != names.len() {
		return Err(SdkError::Other(format!(
			"a published row holds {} values, but the output row has {} columns",
			values.len(),
			names.len()
		)));
	}
	let columns = Columns::from_rows(&names, &[values.to_vec()]);
	R::decode_from(&InProcessRowView::new(&columns, 0))
		.ok_or_else(|| SdkError::Other("a published row does not decode as the output row".to_string()))
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, datetime::DateTime};

	use super::{row_to_values, values_to_row};
	use crate::row;

	#[derive(Debug, Clone, PartialEq)]
	struct Published {
		group: String,
		count: u64,
		mean: f64,
		start: DateTime,
		best: Option<i64>,
	}

	row!(Published {
		group: String,
		count: u64,
		mean: f64,
		start: DateTime,
		best: Option<i64>
	});

	#[test]
	fn a_row_round_trips_through_its_stored_values() {
		// A published row that does not come back intact makes the next update retract a value downstream never saw.
		for row in [
			Published {
				group: "BTC".to_string(),
				count: 7,
				mean: 1.5,
				start: DateTime::from_epoch_millis(1_700_000_000_123).unwrap(),
				best: Some(-3),
			},
			Published {
				group: String::new(),
				count: 0,
				mean: 0.0,
				start: DateTime::default(),
				best: None,
			},
		] {
			let values = row_to_values(&row).unwrap();
			assert_eq!(values.len(), 5);
			assert_eq!(values[0], Value::Utf8(row.group.clone()));
			assert_eq!(values[1], Value::Uint8(row.count));
			assert_eq!(values_to_row::<Published>(&values).unwrap(), row);
		}
	}

	#[test]
	fn values_of_the_wrong_width_fail_to_decode() {
		// A stored row from another output shape decoded by position would publish columns under the wrong names.
		assert!(values_to_row::<Published>(&[Value::Uint8(1)]).is_err());
	}
}
