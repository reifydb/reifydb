// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
