// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey, value::decode_value};
use reifydb_core::{
	interface::catalog::identity::IdentityAttributeValue,
	key::{EncodableKey, identity_attribute_value::IdentityAttributeValueKey, kind::KeyKind},
	return_internal_error,
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
		let value = decode_identity_attribute_value(row)?;
		let version = txn.version();
		let Some(attribute) = catalog.cache.find_identity_attribute_at(value.attribute, version) else {
			return_internal_error!(
				"replicated identity attribute value references unknown attribute {}",
				value.attribute
			);
		};
		if value.value.get_type() != attribute.value_type {
			return_internal_error!(
				"replicated identity attribute value for `{}` has type {}, catalog declares {}",
				attribute.name,
				value.value.get_type(),
				attribute.value_type
			);
		}
		catalog.cache.set_identity_attribute_value(value.identity, value.attribute, version, Some(value));
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

fn decode_identity_attribute_value(row: &EncodedRow) -> Result<IdentityAttributeValue> {
	let identity = identity_attribute_value::SHAPE.get_identity_id(row, identity_attribute_value::IDENTITY);
	let attribute = identity_attribute_value::SHAPE.get_u64(row, identity_attribute_value::ATTRIBUTE);
	let blob = identity_attribute_value::SHAPE.get_blob(row, identity_attribute_value::VALUE);
	let value = match decode_value(blob.as_bytes()) {
		Ok(value) => value,
		Err(e) => return_internal_error!("failed to decode replicated identity attribute value: {}", e),
	};

	Ok(IdentityAttributeValue {
		identity,
		attribute,
		value,
	})
}
