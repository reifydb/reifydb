// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_sdk::common::extern_wasm::marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_value::{
	fragment::Fragment,
	value::{datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns},
};

trait Unmarshalled {
	// Must accept a bare Columns and a Result alike, otherwise a fallible unmarshal stops these tests compiling.
	fn outcome(self) -> Result<Columns, String>;
}

impl Unmarshalled for Columns {
	fn outcome(self) -> Result<Columns, String> {
		Ok(self)
	}
}

impl<E: Display> Unmarshalled for Result<Columns, E> {
	fn outcome(self) -> Result<Columns, String> {
		self.map_err(|error| error.to_string())
	}
}

#[test]
fn rows_with_row_numbers_never_come_back_from_the_wasm_marshal_stamped_at_the_epoch() {
	// A row timestamp the guest never sent must not read back as 1970, which passes for a real time.
	let stamp = DateTime::from_nanos(1_700_000_000_000_000_000);
	let input = Columns::with_system(
		vec![ColumnWithName::new(Fragment::internal("c"), ColumnBuffer::int4([1, 2]))],
		SystemColumns::new(
			vec![RowNumber(1), RowNumber(2)],
			Vec::new(),
			vec![stamp; 2],
			vec![stamp; 2],
			vec![stamp; 2],
			Vec::new(),
		),
	);
	let bytes = marshal_columns_to_bytes(&input).expect("an int4 column with row numbers marshals");
	let output = unmarshal_columns_from_bytes(&bytes).outcome().expect("well formed bytes unmarshal");
	assert_eq!(output.row_numbers(), &[RowNumber(1), RowNumber(2)], "row numbers survive the round trip");
	let epoch = DateTime::from_nanos(0);
	for (name, stamps) in
		[("created_at", output.created_at()), ("updated_at", output.updated_at()), ("time", output.time())]
	{
		assert!(!stamps.contains(&epoch), "{name} reads back as the epoch: {stamps:?}");
	}
}
