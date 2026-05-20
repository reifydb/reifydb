// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{column::Column, id::PrimaryKeyId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimaryKey {
	pub id: PrimaryKeyId,
	pub columns: Vec<Column>,
}
