// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, TestId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Test {
	pub id: TestId,
	pub namespace: NamespaceId,
	pub name: String,
	pub cases: Option<String>,
	pub body: String,
}
