// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{dictionary::Dictionary, id::NamespaceId},
	key::{EncodableKey, dictionary::DictionaryKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{dictionary::DictionaryId, r#type::Type};

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::dictionary::shape::dictionary::{ID, ID_TYPE, NAME, NAMESPACE, SHAPE, VALUE_TYPE},
};

pub(super) struct DictionaryApplier;

impl CatalogChangeApplier for DictionaryApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let dict = decode_dictionary(row);
		catalog.materialized.set_dictionary(dict.id, txn.version(), Some(dict));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = DictionaryKey::decode(key).map(|k| k.dictionary).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::Dictionary,
			},
		)?;
		catalog.materialized.set_dictionary(id, txn.version(), None);
		Ok(())
	}
}

fn decode_dictionary(row: &EncodedRow) -> Dictionary {
	let id = DictionaryId(SHAPE.get_u64(row, ID));
	let namespace = NamespaceId(SHAPE.get_u64(row, NAMESPACE));
	let name = SHAPE.get_utf8(row, NAME).to_string();
	let value_type = Type::from_u8(SHAPE.get_u8(row, VALUE_TYPE));
	let id_type = Type::from_u8(SHAPE.get_u8(row, ID_TYPE));

	Dictionary {
		id,
		namespace,
		name,
		value_type,
		id_type,
	}
}
