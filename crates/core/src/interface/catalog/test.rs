// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, TestId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestDef {
	pub id: TestId,
	pub namespace: NamespaceId,
	pub name: String,
	pub body: String,
}
