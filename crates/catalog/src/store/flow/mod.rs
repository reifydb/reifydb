// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;
pub mod update;

use reifydb_core::interface::catalog::{
	flow::{Flow, FlowId, FlowStatus},
	id::NamespaceId,
};

use crate::store::flow::shape::flow;

pub(crate) fn decode_flow(bytes: &EncodedCatalogRow) -> Flow {
	Flow {
		id: FlowId(flow::get_id(bytes)),
		namespace: NamespaceId(flow::get_namespace(bytes)),
		name: flow::get_name(bytes).to_string(),
		status: FlowStatus::from_u8(flow::get_status(bytes)),
	}
}
