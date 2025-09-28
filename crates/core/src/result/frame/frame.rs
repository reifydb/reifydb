// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::{Deref, Index};

use reifydb_type::RowNumber;
use serde::{Deserialize, Serialize};

use crate::FrameColumn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
	pub row_numbers: Vec<RowNumber>,
	pub columns: Vec<FrameColumn>,
}

impl Deref for Frame {
	type Target = [FrameColumn];

	fn deref(&self) -> &Self::Target {
		&self.columns
	}
}

impl Index<usize> for Frame {
	type Output = FrameColumn;

	fn index(&self, index: usize) -> &Self::Output {
		self.columns.index(index)
	}
}

impl Frame {
	pub fn new(columns: Vec<FrameColumn>) -> Self {
		Self {
			row_numbers: Vec::new(),
			columns,
		}
	}

	pub fn with_row_numbers(columns: Vec<FrameColumn>, row_numbers: Vec<RowNumber>) -> Self {
		Self {
			row_numbers,
			columns,
		}
	}
}
