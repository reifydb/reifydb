// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::{Deref, Index};

use serde::{Deserialize, Serialize};

use crate::FrameColumn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame(pub Vec<FrameColumn>);

impl Deref for Frame {
	type Target = [FrameColumn];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Index<usize> for Frame {
	type Output = FrameColumn;

	fn index(&self, index: usize) -> &Self::Output {
		self.0.index(index)
	}
}

impl Frame {
	pub fn new(columns: Vec<FrameColumn>) -> Self {
		Self(columns)
	}
}
