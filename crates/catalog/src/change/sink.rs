// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SinkId},
		sink::Sink,
	},
	key::{EncodableKey, kind::KeyKind, sink::SinkKey},
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::sink::shape::sink};

pub(super) struct SinkApplier;

impl CatalogChangeApplier for SinkApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let s = decode_sink(EncodedCatalogRow::view(bytes));
		catalog.cache.set_sink(s.id, txn.version(), Some(s));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SinkKey::decode(key).map(|k| k.sink).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Sink,
		})?;
		catalog.cache.set_sink(id, txn.version(), None);
		Ok(())
	}
}

fn decode_sink(bytes: &EncodedCatalogRow) -> Sink {
	let id = SinkId(sink::get_id(bytes));
	let namespace = NamespaceId(sink::get_namespace(bytes));
	let name = sink::get_name(bytes).to_string();
	let source_namespace = NamespaceId(sink::get_source_namespace(bytes));
	let source_name = sink::get_source_name(bytes).to_string();
	let connector = sink::get_connector(bytes).to_string();
	let config_json = sink::get_config(bytes);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let status = FlowStatus::from_u8(sink::get_status(bytes));

	Sink {
		id,
		namespace,
		name,
		source_namespace,
		source_name,
		connector,
		config,
		status,
	}
}
