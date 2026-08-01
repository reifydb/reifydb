// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::flow::{FlowId, Operator, OperatorId},
	key::{EncodableKey, kind::KeyKind, operator::OperatorKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::operator::shape::operator::{self, DATA, FLOW, ID, TYPE},
};

pub(super) struct OperatorApplier;

impl CatalogChangeApplier for OperatorApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let node = decode_operator(row);
		catalog.cache.set_operator(node.id, txn.version(), Some(node));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = OperatorKey::decode(key).map(|k| k.node).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Operator,
		})?;
		catalog.cache.set_operator(id, txn.version(), None);
		Ok(())
	}
}

fn decode_operator(row: &EncodedRow) -> Operator {
	let id = OperatorId(operator::SHAPE.get::<u64>(row, ID));
	let flow = FlowId(operator::SHAPE.get::<u64>(row, FLOW));
	let node_type = operator::SHAPE.get::<u8>(row, TYPE);
	let data = operator::SHAPE.get_blob(row, DATA).clone();

	Operator {
		id,
		flow,
		node_type,
		data,
	}
}
