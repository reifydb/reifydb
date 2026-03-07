// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{column::ColumnDef, id::PrimaryKeyId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKeyDef {
	pub id: PrimaryKeyId,
	pub columns: Vec<ColumnDef>,
}
