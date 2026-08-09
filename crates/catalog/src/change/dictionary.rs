// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
	tag::value_type_from_tag_byte,
};
use reifydb_core::{
	interface::catalog::{dictionary::Dictionary, id::NamespaceId},
	key::{EncodableKey, dictionary::DictionaryKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::dictionary::DictionaryId;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::dictionary::shape::dictionary};

pub(super) struct DictionaryApplier;

impl CatalogChangeApplier for DictionaryApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let dict = decode_dictionary(EncodedCatalogRow::view(bytes));
		catalog.cache.set_dictionary(dict.id, txn.version(), Some(dict));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = DictionaryKey::decode(key).map(|k| k.dictionary).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::Dictionary,
			},
		)?;
		catalog.cache.set_dictionary(id, txn.version(), None);
		Ok(())
	}
}

fn decode_dictionary(bytes: &EncodedCatalogRow) -> Dictionary {
	let id = DictionaryId(dictionary::get_id(bytes));
	let namespace = NamespaceId(dictionary::get_namespace(bytes));
	let name = dictionary::get_name(bytes).to_string();
	let value_type = value_type_from_tag_byte(dictionary::get_value_type(bytes));
	let id_type = value_type_from_tag_byte(dictionary::get_id_type(bytes));

	Dictionary {
		id,
		namespace,
		name,
		value_type,
		id_type,
	}
}
