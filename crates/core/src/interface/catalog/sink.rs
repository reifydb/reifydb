// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	flow::FlowStatus,
	id::{NamespaceId, SinkId},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SinkDef {
	pub id: SinkId,
	pub namespace: NamespaceId,
	pub name: String,
	pub source_namespace: NamespaceId,
	pub source_name: String,
	pub connector: String,
	pub config: Vec<(String, String)>,
	pub status: FlowStatus,
}
