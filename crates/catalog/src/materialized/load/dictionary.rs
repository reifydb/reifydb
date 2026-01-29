// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			dictionary::DictionaryDef,
			id::{DictionaryId, NamespaceId},
		},
		store::MultiVersionValues,
	},
	key::dictionary::DictionaryKey,
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::value::r#type::Type;

use super::MaterializedCatalog;
use crate::store::dictionary::schema::dictionary::{ID, ID_TYPE, NAME, NAMESPACE, SCHEMA, VALUE_TYPE};

pub(crate) fn load_dictionaries(rx: &mut impl AsTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let mut txn = rx.as_transaction();
	let range = DictionaryKey::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let dict_def = convert_dictionary(multi);
		catalog.set_dictionary(dict_def.id, version, Some(dict_def));
	}

	Ok(())
}

fn convert_dictionary(multi: MultiVersionValues) -> DictionaryDef {
	let row = multi.values;
	let id = DictionaryId(SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(SCHEMA.get_u64(&row, NAMESPACE));
	let name = SCHEMA.get_utf8(&row, NAME).to_string();
	let value_type_ordinal = SCHEMA.get_u8(&row, VALUE_TYPE);
	let id_type_ordinal = SCHEMA.get_u8(&row, ID_TYPE);

	DictionaryDef {
		id,
		namespace,
		name,
		value_type: Type::from_u8(value_type_ordinal),
		id_type: Type::from_u8(id_type_ordinal),
	}
}
