// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::frame::data::FrameColumnData;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameColumn {
	pub namespace: Option<String>,
	pub store: Option<String>,
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
		match (&self.namespace, &self.store) {
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
