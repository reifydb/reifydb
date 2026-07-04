// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::identity::IdentityAttributeValue,
	key::{EncodableKey, identity_attribute_value::IdentityAttributeValueKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result, catalog::Catalog, error::CatalogChangeError,
	store::identity_attribute_value::shape::identity_attribute_value,
};

pub(super) struct IdentityAttributeValueApplier;

impl CatalogChangeApplier for IdentityAttributeValueApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let value = decode_identity_attribute_value(row);
		catalog.cache.set_identity_attribute_value(value.identity, value.attribute, txn.version(), Some(value));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let k = IdentityAttributeValueKey::decode(key).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::IdentityAttributeValue,
		})?;
		catalog.cache.set_identity_attribute_value(k.identity, k.attribute, txn.version(), None);
		Ok(())
	}
}

fn decode_identity_attribute_value(row: &EncodedRow) -> IdentityAttributeValue {
	let identity = identity_attribute_value::SHAPE.get_identity_id(row, identity_attribute_value::IDENTITY);
	let attribute = identity_attribute_value::SHAPE.get_u64(row, identity_attribute_value::ATTRIBUTE);
	let value = identity_attribute_value::SHAPE.get_utf8(row, identity_attribute_value::VALUE).to_string();

	IdentityAttributeValue {
		identity,
		attribute,
		value,
	}
}
