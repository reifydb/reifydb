// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::identity::IdentityAttribute,
	key::{EncodableKey, identity_attribute::IdentityAttributeKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use super::CatalogChangeApplier;
use crate::{
	Result, catalog::Catalog, error::CatalogChangeError, store::identity_attribute::shape::identity_attribute,
};

pub(super) struct IdentityAttributeApplier;

impl CatalogChangeApplier for IdentityAttributeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let attribute = decode_identity_attribute(row);
		catalog.cache.set_identity_attribute(attribute.id, txn.version(), Some(attribute));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let k = IdentityAttributeKey::decode(key).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::IdentityAttribute,
		})?;
		catalog.cache.set_identity_attribute(k.attribute, txn.version(), None);
		Ok(())
	}
}

fn decode_identity_attribute(row: &EncodedRow) -> IdentityAttribute {
	let id = identity_attribute::SHAPE.get_u64(row, identity_attribute::ID);
	let name = identity_attribute::SHAPE.get_utf8(row, identity_attribute::NAME).to_string();
	let value_type = ValueType::from_u8(identity_attribute::SHAPE.get_u8(row, identity_attribute::VALUE_TYPE));

	IdentityAttribute {
		id,
		name,
		value_type,
	}
}
