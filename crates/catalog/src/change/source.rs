// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
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
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::source::shape::source::{
		self, CONFIG, CONNECTOR, ID, NAME, NAMESPACE, STATUS, TARGET_NAME, TARGET_NAMESPACE,
	},
};

pub(super) struct SourceApplier;

impl CatalogChangeApplier for SourceApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let src = decode_source(row);
		catalog.materialized.set_source(src.id, txn.version(), Some(src));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SourceKey::decode(key).map(|k| k.source).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Source,
		})?;
		catalog.materialized.set_source(id, txn.version(), None);
		Ok(())
	}
}

fn decode_source(row: &EncodedRow) -> Source {
	let id = SourceId(source::SHAPE.get_u64(row, ID));
	let namespace = NamespaceId(source::SHAPE.get_u64(row, NAMESPACE));
	let name = source::SHAPE.get_utf8(row, NAME).to_string();
	let connector = source::SHAPE.get_utf8(row, CONNECTOR).to_string();
	let config_json = source::SHAPE.get_utf8(row, CONFIG);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let target_namespace = NamespaceId(source::SHAPE.get_u64(row, TARGET_NAMESPACE));
	let target_name = source::SHAPE.get_utf8(row, TARGET_NAME).to_string();
	let status = FlowStatus::from_u8(source::SHAPE.get_u8(row, STATUS));

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
