// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId, OperatorId},
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
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let edge = decode_flow_edge(bytes);
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

fn decode_flow_edge(bytes: &EncodedBytes) -> FlowEdge {
	let id = FlowEdgeId(flow_edge::SHAPE.get::<u64>(bytes, ID));
	let flow = FlowId(flow_edge::SHAPE.get::<u64>(bytes, FLOW));
	let source = OperatorId(flow_edge::SHAPE.get::<u64>(bytes, SOURCE));
	let target = OperatorId(flow_edge::SHAPE.get::<u64>(bytes, TARGET));

	FlowEdge {
		id,
		flow,
		source,
		target,
	}
}
