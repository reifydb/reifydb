// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	DictionaryDef, DictionaryId, DictionaryKey, MultiVersionQueryTransaction, MultiVersionValues, NamespaceId,
};
use reifydb_type::Type;

use crate::{
	MaterializedCatalog,
	store::dictionary::layout::dictionary::{ID, ID_TYPE, LAYOUT, NAME, NAMESPACE, VALUE_TYPE},
};

pub(crate) async fn load_dictionaries(
	qt: &mut impl MultiVersionQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = DictionaryKey::full_scan();
	let batch = qt.range(range).await?;

	for multi in batch.items {
		let version = multi.version;
		let dict_def = convert_dictionary(multi);
		catalog.set_dictionary(dict_def.id, version, Some(dict_def));
	}

	Ok(())
}

fn convert_dictionary(multi: MultiVersionValues) -> DictionaryDef {
	let row = multi.values;
	let id = DictionaryId(LAYOUT.get_u64(&row, ID));
	let namespace = NamespaceId(LAYOUT.get_u64(&row, NAMESPACE));
	let name = LAYOUT.get_utf8(&row, NAME).to_string();
	let value_type_ordinal = LAYOUT.get_u8(&row, VALUE_TYPE);
	let id_type_ordinal = LAYOUT.get_u8(&row, ID_TYPE);

	DictionaryDef {
		id,
		namespace,
		name,
		value_type: Type::from_u8(value_type_ordinal),
		id_type: Type::from_u8(id_type_ordinal),
	}
}
