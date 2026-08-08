// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;
pub mod update;

use reifydb_codec::encoded::bytes::EncodedBytes;
use reifydb_core::interface::catalog::{
	flow::{Flow, FlowId, FlowStatus},
	id::NamespaceId,
};

use crate::store::flow::shape::flow;

pub(crate) fn decode_flow(bytes: &EncodedBytes) -> Flow {
	Flow {
		id: FlowId(flow::SHAPE.get::<u64>(bytes, flow::ID)),
		namespace: NamespaceId(flow::SHAPE.get::<u64>(bytes, flow::NAMESPACE)),
		name: flow::SHAPE.get_utf8(bytes, flow::NAME).to_string(),
		status: FlowStatus::from_u8(flow::SHAPE.get::<u8>(bytes, flow::STATUS)),
	}
}
