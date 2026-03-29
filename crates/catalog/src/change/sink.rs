// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
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
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::sink::schema::sink::{
		self, CONFIG, CONNECTOR, ID, NAME, NAMESPACE, SOURCE_NAME, SOURCE_NAMESPACE, STATUS,
	},
};

pub(super) struct SinkApplier;

impl CatalogChangeApplier for SinkApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let s = decode_sink(row);
		catalog.materialized.set_sink(s.id, txn.version(), Some(s));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SinkKey::decode(key).map(|k| k.sink).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Sink,
		})?;
		catalog.materialized.set_sink(id, txn.version(), None);
		Ok(())
	}
}

fn decode_sink(row: &EncodedRow) -> Sink {
	let id = SinkId(sink::SCHEMA.get_u64(row, ID));
	let namespace = NamespaceId(sink::SCHEMA.get_u64(row, NAMESPACE));
	let name = sink::SCHEMA.get_utf8(row, NAME).to_string();
	let source_namespace = NamespaceId(sink::SCHEMA.get_u64(row, SOURCE_NAMESPACE));
	let source_name = sink::SCHEMA.get_utf8(row, SOURCE_NAME).to_string();
	let connector = sink::SCHEMA.get_utf8(row, CONNECTOR).to_string();
	let config_json = sink::SCHEMA.get_utf8(row, CONFIG);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let status = FlowStatus::from_u8(sink::SCHEMA.get_u8(row, STATUS));

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
