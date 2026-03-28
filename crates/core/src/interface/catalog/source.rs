// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	flow::FlowStatus,
	id::{NamespaceId, SourceId},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
	pub id: SourceId,
	pub namespace: NamespaceId,
	pub name: String,
	pub connector: String,
	pub config: Vec<(String, String)>,
	pub target_namespace: NamespaceId,
	pub target_name: String,
	pub status: FlowStatus,
}
