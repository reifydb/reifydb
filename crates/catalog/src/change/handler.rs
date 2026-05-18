// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		handler::Handler,
		id::{HandlerId, NamespaceId},
	},
	key::{EncodableKey, handler::HandlerKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::{SumTypeId, VariantRef};

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::handler::shape::handler::{self, BODY_SOURCE, ID, NAME, NAMESPACE, ON_SUMTYPE_ID, ON_VARIANT_TAG},
};

pub(super) struct HandlerApplier;

impl CatalogChangeApplier for HandlerApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let handler = decode_handler(row);
		catalog.cache.set_handler(handler.id, txn.version(), Some(handler));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = HandlerKey::decode(key).map(|k| k.handler).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Handler,
		})?;
		catalog.cache.set_handler(id, txn.version(), None);
		Ok(())
	}
}

fn decode_handler(row: &EncodedRow) -> Handler {
	let id = HandlerId(handler::SHAPE.get_u64(row, ID));
	let namespace = NamespaceId(handler::SHAPE.get_u64(row, NAMESPACE));
	let name = handler::SHAPE.get_utf8(row, NAME).to_string();
	let sumtype_id = SumTypeId(handler::SHAPE.get_u64(row, ON_SUMTYPE_ID));
	let variant_tag = handler::SHAPE.get_u8(row, ON_VARIANT_TAG);
	let body_source = handler::SHAPE.get_utf8(row, BODY_SOURCE).to_string();

	Handler {
		id,
		namespace,
		name,
		variant: VariantRef {
			sumtype_id,
			variant_tag,
		},
		body_source,
	}
}
