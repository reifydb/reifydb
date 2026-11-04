// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, SourceId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
	pub id: SourceId,
	pub namespace: NamespaceId,
	pub name: String,
	pub connector: String,
	pub config: Vec<(String, String)>,
	pub target_namespace: NamespaceId,
	pub target_name: String,
}
