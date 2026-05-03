// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		flow::{Flow, FlowId, FlowStatus},
		id::NamespaceId,
	},
	key::{EncodableKey, flow::FlowKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::duration::Duration;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::flow::shape::flow::{self, ID, NAME, NAMESPACE, STATUS, TICK_NANOS},
};

pub(super) struct FlowApplier;

impl CatalogChangeApplier for FlowApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let flow = decode_flow(row);
		catalog.cache.set_flow(flow.id, txn.version(), Some(flow));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = FlowKey::decode(key).map(|k| k.flow).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Flow,
		})?;
		catalog.cache.set_flow(id, txn.version(), None);
		Ok(())
	}
}

fn decode_flow(row: &EncodedRow) -> Flow {
	let id = FlowId(flow::SHAPE.get_u64(row, ID));
	let namespace = NamespaceId(flow::SHAPE.get_u64(row, NAMESPACE));
	let name = flow::SHAPE.get_utf8(row, NAME).to_string();
	let status = FlowStatus::from_u8(flow::SHAPE.get_u8(row, STATUS));
	let tick_nanos = flow::SHAPE.get_u64(row, TICK_NANOS);
	let tick = if tick_nanos > 0 {
		Some(Duration::from_nanoseconds(tick_nanos as i64).unwrap())
	} else {
		None
	};

	Flow {
		id,
		namespace,
		name,
		status,
		tick,
	}
}
