// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnDef, PrimaryKeyId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKeyDef {
	pub id: PrimaryKeyId,
	pub columns: Vec<ColumnDef>,
}
