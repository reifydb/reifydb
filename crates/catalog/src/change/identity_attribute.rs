// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes, tag::value_type_from_tag_byte};
use reifydb_core::{
	interface::catalog::identity::IdentityAttribute,
	key::{EncodableKey, identity_attribute::IdentityAttributeKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result, catalog::Catalog, error::CatalogChangeError, store::identity_attribute::shape::identity_attribute,
};

pub(super) struct IdentityAttributeApplier;

impl CatalogChangeApplier for IdentityAttributeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let attribute = decode_identity_attribute(bytes);
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

fn decode_identity_attribute(bytes: &EncodedBytes) -> IdentityAttribute {
	let id = identity_attribute::get_id(bytes);
	let name = identity_attribute::get_name(bytes).to_string();
	let value_type = value_type_from_tag_byte(identity_attribute::get_value_type(bytes));

	IdentityAttribute {
		id,
		name,
		value_type,
	}
}
