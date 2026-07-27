// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::key::{EncodableKey, flow::FlowKey, kind::KeyKind};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::flow::decode_flow,
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
