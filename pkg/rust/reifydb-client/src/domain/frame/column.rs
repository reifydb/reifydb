// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{Type, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameColumn {
	pub schema: Option<String>,
	pub store: Option<String>,
	pub name: String,
	pub r#type: Type,
	pub data: Vec<Value>,
}

impl Deref for FrameColumn {
	type Target = Vec<Value>;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl FrameColumn {
	pub fn qualified_name(&self) -> String {
		match (&self.schema, &self.store) {
			(Some(schema), Some(table)) => {
				format!("{}.{}.{}", schema, table, self.name)
			}
			(None, Some(table)) => {
				format!("{}.{}", table, self.name)
			}
			_ => self.name.clone(),
		}
	}
}
