// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::flow::{FlowId, FlowNode, FlowNodeId},
	key::{EncodableKey, flow_node::FlowNodeKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::flow_node::shape::flow_node::{self, DATA, FLOW, ID, TYPE},
};

pub(super) struct FlowNodeApplier;

impl CatalogChangeApplier for FlowNodeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let node = decode_flow_node(row);
		catalog.cache.set_flow_node(node.id, txn.version(), Some(node));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = FlowNodeKey::decode(key).map(|k| k.node).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::FlowNode,
		})?;
		catalog.cache.set_flow_node(id, txn.version(), None);
		Ok(())
	}
}

fn decode_flow_node(row: &EncodedRow) -> FlowNode {
	let id = FlowNodeId(flow_node::SHAPE.get_u64(row, ID));
	let flow = FlowId(flow_node::SHAPE.get_u64(row, FLOW));
	let node_type = flow_node::SHAPE.get_u8(row, TYPE);
	let data = flow_node::SHAPE.get_blob(row, DATA).clone();

	FlowNode {
		id,
		flow,
		node_type,
		data,
	}
}
