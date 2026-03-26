// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{dictionary::DictionaryId, r#type::Type};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::NamespaceId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dictionary {
	pub id: DictionaryId,
	pub namespace: NamespaceId,
	pub name: String,
	pub value_type: Type,
	pub id_type: Type,
}

impl Dictionary {
	pub fn name(&self) -> &str {
		&self.name
	}
}
