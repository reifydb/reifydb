// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		id::NamespaceId,
		sumtype::{SumType, SumTypeKind, Variant},
	},
	key::{EncodableKey, kind::KeyKind, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::sumtype::schema::sumtype::{ID, KIND, NAME, NAMESPACE, SCHEMA, VARIANTS_JSON},
};

pub(super) struct SumTypeApplier;

impl CatalogChangeApplier for SumTypeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let def = decode_sumtype(row);
		catalog.materialized.set_sumtype(def.id, txn.version(), Some(def));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SumTypeKey::decode(key).map(|k| k.sumtype).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::SumType,
		})?;
		catalog.materialized.set_sumtype(id, txn.version(), None);
		Ok(())
	}
}

fn decode_sumtype(row: &EncodedRow) -> SumType {
	let id = SumTypeId(SCHEMA.get_u64(row, ID));
	let namespace = NamespaceId(SCHEMA.get_u64(row, NAMESPACE));
	let name = SCHEMA.get_utf8(row, NAME).to_string();
	let variants_json = SCHEMA.get_utf8(row, VARIANTS_JSON);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});
	let kind = if SCHEMA.get_u8(row, KIND) != 0 {
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
