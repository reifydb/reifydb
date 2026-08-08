// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SourceId},
		source::Source,
	},
	key::{EncodableKey, kind::KeyKind, source::SourceKey},
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::source::shape::source};

pub(super) struct SourceApplier;

impl CatalogChangeApplier for SourceApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let src = decode_source(bytes);
		catalog.cache.set_source(src.id, txn.version(), Some(src));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SourceKey::decode(key).map(|k| k.source).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Source,
		})?;
		catalog.cache.set_source(id, txn.version(), None);
		Ok(())
	}
}

fn decode_source(bytes: &EncodedBytes) -> Source {
	let id = SourceId(source::get_id(bytes));
	let namespace = NamespaceId(source::get_namespace(bytes));
	let name = source::get_name(bytes).to_string();
	let connector = source::get_connector(bytes).to_string();
	let config_json = source::get_config(bytes);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let target_namespace = NamespaceId(source::get_target_namespace(bytes));
	let target_name = source::get_target_name(bytes).to_string();
	let status = FlowStatus::from_u8(source::get_status(bytes));

	Source {
		id,
		namespace,
		name,
		connector,
		config,
		target_namespace,
		target_name,
		status,
	}
}
