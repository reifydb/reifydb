// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error},
	value::{Value, frame::frame::Frame},
};

pub trait ResultAssert<T> {
	fn assert_error(self, code: &str) -> Diagnostic;
}

impl<T> ResultAssert<T> for Result<T, Error> {
	#[track_caller]
	fn assert_error(self, code: &str) -> Diagnostic {
		match self {
			Ok(_) => panic!("expected error '{code}', but the statement succeeded"),
			Err(error) => {
				let diagnostic = error.diagnostic();
				assert_eq!(
					diagnostic.code, code,
					"expected diagnostic '{code}', found '{}'",
					diagnostic.code
				);
				diagnostic
			}
		}
	}
}

pub trait FrameAssert {
	fn assert(&self) -> FrameAssertion<'_>;
}

impl FrameAssert for Frame {
	fn assert(&self) -> FrameAssertion<'_> {
		FrameAssertion {
			frame: self,
		}
	}
}

impl FrameAssert for [Frame] {
	fn assert(&self) -> FrameAssertion<'_> {
		let frame = self.first().unwrap_or_else(|| panic!("expected at least one frame, found none"));
		FrameAssertion {
			frame,
		}
	}
}

pub struct FrameAssertion<'a> {
	frame: &'a Frame,
}

impl<'a> FrameAssertion<'a> {
	pub fn row_count(&self, expected: usize) -> &Self {
		let actual = self.frame.row_count();
		assert_eq!(actual, expected, "expected {expected} rows, found {actual}");
		self
	}

	pub fn is_empty(&self) -> &Self {
		self.row_count(0)
	}

	pub fn column_ascending(&self, name: &str) -> &Self {
		let values = column_values(self.frame, name);
		for pair in values.windows(2) {
			assert!(
				pair[0] <= pair[1],
				"column '{name}' is not ascending: {:?} comes before {:?}",
				pair[0],
				pair[1]
			);
		}
		self
	}

	pub fn column_descending(&self, name: &str) -> &Self {
		let values = column_values(self.frame, name);
		for pair in values.windows(2) {
			assert!(
				pair[0] >= pair[1],
				"column '{name}' is not descending: {:?} comes before {:?}",
				pair[0],
				pair[1]
			);
		}
		self
	}

	pub fn column(&self, name: &str, expected: &[Value]) -> &Self {
		let actual = column_values(self.frame, name);
		assert_eq!(
			actual.as_slice(),
			expected,
			"column '{name}' mismatch: expected {expected:?}, found {actual:?}"
		);
		self
	}

	pub fn row(&self, index: usize) -> RowAssertion {
		let rows = self.frame.to_rows();
		assert!(index < rows.len(), "row index {index} out of range (total: {})", rows.len());
		RowAssertion {
			row: rows.into_iter().nth(index).unwrap(),
			index,
		}
	}
}

pub struct RowAssertion {
	row: Vec<(String, Value)>,
	index: usize,
}

impl RowAssertion {
	pub fn value(&self, column: &str, expected: Value) -> &Self {
		let actual =
			self.row.iter()
				.find(|(name, _)| name == column)
				.map(|(_, value)| value.clone())
				.unwrap_or_else(|| panic!("row {} has no column '{column}'", self.index));
		assert_eq!(
			actual, expected,
			"row {} column '{column}' mismatch: expected {expected:?}, found {actual:?}",
			self.index
		);
		self
	}
}

pub fn column_values(frame: &Frame, name: &str) -> Vec<Value> {
	frame.to_rows()
		.into_iter()
		.map(|row| {
			row.into_iter()
				.find(|(column, _)| column == name)
				.map(|(_, value)| value)
				.unwrap_or_else(|| panic!("frame has no column '{name}'"))
		})
		.collect()
}

pub fn rows(frames: &[Frame]) -> Vec<Vec<(String, Value)>> {
	frames.iter().flat_map(|frame| frame.to_rows()).collect()
}

pub fn assert_same_rows(actual: &[Frame], expected: &[Frame]) {
	let sort_key = |rows: Vec<Vec<(String, Value)>>| {
		let mut rows = rows;
		rows.sort_by_key(|row| format!("{row:?}"));
		rows
	};
	let actual_rows = sort_key(rows(actual));
	let expected_rows = sort_key(rows(expected));
	assert_eq!(
		actual_rows, expected_rows,
		"row sets differ:\n  actual:   {actual_rows:?}\n  expected: {expected_rows:?}"
	);
}

pub fn assert_frames_eq(actual: &[Frame], expected: &[Frame]) {
	assert_eq!(
		actual.len(),
		expected.len(),
		"frame count mismatch: expected {}, found {}",
		expected.len(),
		actual.len()
	);
	for (index, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
		assert_eq!(
			a.to_rows(),
			e.to_rows(),
			"frame {index} mismatch: expected {:?}, found {:?}",
			e.to_rows(),
			a.to_rows()
		);
	}
}
