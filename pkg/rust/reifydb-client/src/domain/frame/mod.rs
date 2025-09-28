// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::ops::{Deref, Index};

use serde::{Deserialize, Serialize};

mod column;
mod display;

pub use column::FrameColumn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
	#[serde(default)]
	pub row_numbers: Vec<u64>,
	#[serde(default)]
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
	pub fn new(row_numbers: Vec<u64>, columns: Vec<FrameColumn>) -> Self {
		Self {
			row_numbers,
			columns,
		}
	}
}
