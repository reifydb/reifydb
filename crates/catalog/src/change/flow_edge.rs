// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId, FlowNodeId},
	key::{EncodableKey, flow_edge::FlowEdgeKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::flow_edge::shape::flow_edge::{self, FLOW, ID, SOURCE, TARGET},
};

pub(super) struct FlowEdgeApplier;

impl CatalogChangeApplier for FlowEdgeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let edge = decode_flow_edge(row);
		catalog.cache.set_flow_edge(edge.id, txn.version(), Some(edge));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = FlowEdgeKey::decode(key).map(|k| k.edge).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::FlowEdge,
		})?;
		catalog.cache.set_flow_edge(id, txn.version(), None);
		Ok(())
	}
}

fn decode_flow_edge(row: &EncodedRow) -> FlowEdge {
	let id = FlowEdgeId(flow_edge::SHAPE.get_u64(row, ID));
	let flow = FlowId(flow_edge::SHAPE.get_u64(row, FLOW));
	let source = FlowNodeId(flow_edge::SHAPE.get_u64(row, SOURCE));
	let target = FlowNodeId(flow_edge::SHAPE.get_u64(row, TARGET));

	FlowEdge {
		id,
		flow,
		source,
		target,
	}
}
