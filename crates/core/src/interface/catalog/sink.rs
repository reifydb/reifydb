// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	flow::FlowStatus,
	id::{NamespaceId, SinkId},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sink {
	pub id: SinkId,
	pub namespace: NamespaceId,
	pub name: String,
	pub source_namespace: NamespaceId,
	pub source_name: String,
	pub connector: String,
	pub config: Vec<(String, String)>,
	pub status: FlowStatus,
}
