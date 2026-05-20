// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
