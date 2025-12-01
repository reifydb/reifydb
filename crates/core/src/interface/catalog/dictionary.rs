// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Type;
use serde::{Deserialize, Serialize};

use crate::interface::{DictionaryId, NamespaceId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DictionaryDef {
	pub id: DictionaryId,
	pub namespace: NamespaceId,
	pub name: String,
	pub value_type: Type,
	pub id_type: Type,
}
