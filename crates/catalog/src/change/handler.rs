// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::catalog::{
		handler::Handler,
		id::{HandlerId, NamespaceId},
	},
	key::{EncodableKey, handler::HandlerKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::sumtype::{SumTypeId, VariantRef};

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::handler::shape::handler};

pub(super) struct HandlerApplier;

impl CatalogChangeApplier for HandlerApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let handler = decode_handler(bytes);
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

fn decode_handler(bytes: &EncodedBytes) -> Handler {
	let id = HandlerId(handler::get_id(bytes));
	let namespace = NamespaceId(handler::get_namespace(bytes));
	let name = handler::get_name(bytes).to_string();
	let sumtype_id = SumTypeId(handler::get_on_sumtype_id(bytes));
	let variant_tag = handler::get_on_variant_tag(bytes);
	let body_source = handler::get_body_source(bytes).to_string();

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
