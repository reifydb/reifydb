// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{row::catalog::EncodedCatalogRow, tag::value_type_from_tag_byte};
use reifydb_core::{
	interface::{
		catalog::{dictionary::Dictionary, id::NamespaceId},
		store::MultiVersionRow,
	},
	key::dictionary::DictionaryKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::dictionary::DictionaryId;

use super::CatalogCache;
use crate::{Result, store::dictionary::shape::dictionary};

pub(crate) fn load_dictionaries(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = DictionaryKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let dict_def = convert_dictionary(multi)?;
		catalog.set_dictionary(dict_def.id, version, Some(dict_def));
	}

	Ok(())
}

fn convert_dictionary(multi: MultiVersionRow) -> Result<Dictionary> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = DictionaryId(dictionary::get_id(&bytes));
	let namespace = NamespaceId(dictionary::get_namespace(&bytes));
	let name = dictionary::get_name(&bytes).to_string();
	let value_type_ordinal = dictionary::get_value_type(&bytes);
	let id_type_ordinal = dictionary::get_id_type(&bytes);

	Ok(Dictionary {
		id,
		namespace,
		name,
		value_type: value_type_from_tag_byte(value_type_ordinal),
		id_type: value_type_from_tag_byte(id_type_ordinal),
	})
}
