// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{dictionary::DictionaryId, value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::NamespaceId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dictionary {
	pub id: DictionaryId,
	pub namespace: NamespaceId,
	pub name: String,
	pub value_type: ValueType,
	pub id_type: ValueType,
}

impl Dictionary {
	pub fn name(&self) -> &str {
		&self.name
	}
}
