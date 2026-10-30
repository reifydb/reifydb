// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	flow::FlowId,
	id::{NamespaceId, ViewId, WindowedViewGroupId},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowedViewGroup {
	pub id: WindowedViewGroupId,
	pub namespace: NamespaceId,
	pub name: String,
	pub flow: FlowId,
	pub members: Vec<ViewId>,
}
