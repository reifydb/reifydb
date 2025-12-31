// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use super::data::FrameColumnData;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameColumn {
	pub namespace: Option<String>,
	pub source: Option<String>,
	pub name: String,
	pub data: FrameColumnData,
}

impl Deref for FrameColumn {
	type Target = FrameColumnData;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl FrameColumn {
	pub fn qualified_name(&self) -> String {
		match (&self.namespace, &self.source) {
			(Some(namespace), Some(table)) => {
				format!("{}.{}.{}", namespace, table, self.name)
			}
			(None, Some(table)) => {
				format!("{}.{}", table, self.name)
			}
			_ => self.name.clone(),
		}
	}
}
