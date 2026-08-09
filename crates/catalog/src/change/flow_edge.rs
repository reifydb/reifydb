// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId, OperatorId},
	key::{EncodableKey, flow_edge::FlowEdgeKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::flow_edge::shape::flow_edge};

pub(super) struct FlowEdgeApplier;

impl CatalogChangeApplier for FlowEdgeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let edge = decode_flow_edge(EncodedCatalogRow::view(bytes));
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

fn decode_flow_edge(bytes: &EncodedCatalogRow) -> FlowEdge {
	let id = FlowEdgeId(flow_edge::get_id(bytes));
	let flow = FlowId(flow_edge::get_flow(bytes));
	let source = OperatorId(flow_edge::get_source(bytes));
	let target = OperatorId(flow_edge::get_target(bytes));

	FlowEdge {
		id,
		flow,
		source,
		target,
	}
}
