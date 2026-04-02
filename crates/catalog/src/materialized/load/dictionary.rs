// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{dictionary::Dictionary, id::NamespaceId},
		store::MultiVersionRow,
	},
	key::dictionary::DictionaryKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{dictionary::DictionaryId, r#type::Type};

use super::MaterializedCatalog;
use crate::{
	Result,
	store::dictionary::shape::dictionary::{ID, ID_TYPE, NAME, NAMESPACE, SHAPE, VALUE_TYPE},
};

pub(crate) fn load_dictionaries(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = DictionaryKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let dict_def = convert_dictionary(multi);
		catalog.set_dictionary(dict_def.id, version, Some(dict_def));
	}

	Ok(())
}

fn convert_dictionary(multi: MultiVersionRow) -> Dictionary {
	let row = multi.row;
	let id = DictionaryId(SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(SHAPE.get_u64(&row, NAMESPACE));
	let name = SHAPE.get_utf8(&row, NAME).to_string();
	let value_type_ordinal = SHAPE.get_u8(&row, VALUE_TYPE);
	let id_type_ordinal = SHAPE.get_u8(&row, ID_TYPE);

	Dictionary {
		id,
		namespace,
		name,
		value_type: Type::from_u8(value_type_ordinal),
		id_type: Type::from_u8(id_type_ordinal),
	}
}
