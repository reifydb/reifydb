// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use super::data::FrameColumnData;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameColumn {
	pub name: String,
	pub data: FrameColumnData,
}

impl Deref for FrameColumn {
	type Target = FrameColumnData;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}
