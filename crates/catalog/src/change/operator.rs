// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	interface::catalog::flow::{FlowId, Operator, OperatorId},
	key::{EncodableKey, kind::KeyKind, operator::key::OperatorKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::operator::shape::operator};

pub(super) struct OperatorApplier;

impl CatalogChangeApplier for OperatorApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let operator = decode_operator(EncodedCatalogRow::view(bytes));
		catalog.cache.set_operator(operator.id, txn.version(), Some(operator));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = OperatorKey::decode(key).map(|k| k.operator).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Operator,
		})?;
		catalog.cache.set_operator(id, txn.version(), None);
		Ok(())
	}
}

fn decode_operator(bytes: &EncodedCatalogRow) -> Operator {
	let id = OperatorId(operator::get_id(bytes));
	let flow = FlowId(operator::get_flow(bytes));
	let node_type = operator::get_type(bytes);
	let data = operator::get_data(bytes).clone();

	Operator {
		id,
		flow,
		node_type,
		data,
	}
}
