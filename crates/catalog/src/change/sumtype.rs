// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::{
		id::NamespaceId,
		sumtype::{SumType, SumTypeKind, Variant},
	},
	key::{EncodableKey, kind::KeyKind, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::sumtype::shape::sumtype::{ID, KIND, NAME, NAMESPACE, SHAPE, VARIANTS_JSON},
};

pub(super) struct SumTypeApplier;

impl CatalogChangeApplier for SumTypeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let def = decode_sumtype(bytes);
		catalog.cache.set_sumtype(def.id, txn.version(), Some(def));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SumTypeKey::decode(key).map(|k| k.sumtype).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::SumType,
		})?;
		catalog.cache.set_sumtype(id, txn.version(), None);
		Ok(())
	}
}

fn decode_sumtype(bytes: &EncodedBytes) -> SumType {
	let id = SumTypeId(SHAPE.get::<u64>(bytes, ID));
	let namespace = NamespaceId(SHAPE.get::<u64>(bytes, NAMESPACE));
	let name = SHAPE.get_utf8(bytes, NAME).to_string();
	let variants_json = SHAPE.get_utf8(bytes, VARIANTS_JSON);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});
	let kind = if SHAPE.get::<u8>(bytes, KIND) != 0 {
		SumTypeKind::Event
	} else {
		SumTypeKind::Enum
	};

	SumType {
		id,
		namespace,
		name,
		variants,
		kind,
	}
}
