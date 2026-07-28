// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;
pub mod update;

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{
	common::TimeDomain,
	interface::catalog::{
		flow::{Flow, FlowId, FlowStatus},
		id::NamespaceId,
	},
};

use crate::store::flow::shape::flow;

pub(crate) fn decode_flow(row: &EncodedRow) -> Flow {
	Flow {
		id: FlowId(flow::SHAPE.get::<u64>(row, flow::ID)),
		namespace: NamespaceId(flow::SHAPE.get::<u64>(row, flow::NAMESPACE)),
		name: flow::SHAPE.get_utf8(row, flow::NAME).to_string(),
		status: FlowStatus::from_u8(flow::SHAPE.get::<u8>(row, flow::STATUS)),
		time: TimeDomain::declared_from_u8(flow::SHAPE.get::<u8>(row, flow::TIME)),
	}
}
