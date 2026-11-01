// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{reader::Reader, row::catalog::EncodedCatalogRow, typeinfo::decode_value_type};
use reifydb_core::{
	interface::{
		catalog::{dictionary::Dictionary, id::NamespaceId},
		store::MultiVersionRow,
	},
	key::catalog::DictionaryKey,
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

fn convert_dictionary<K>(multi: MultiVersionRow<K>) -> Result<Dictionary> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = DictionaryId(dictionary::get_id(&bytes));
	let namespace = NamespaceId(dictionary::get_namespace(&bytes));
	let name = dictionary::get_name(&bytes).to_string();
	let value_type = decode_value_type(&mut Reader::new(dictionary::get_value_type(&bytes).as_bytes()))
		.unwrap_or_else(|error| panic!("invalid persisted value type of dictionary {id:?}: {error}"));
	let id_type = decode_value_type(&mut Reader::new(dictionary::get_id_type(&bytes).as_bytes()))
		.unwrap_or_else(|error| panic!("invalid persisted id type of dictionary {id:?}: {error}"));

	Ok(Dictionary {
		id,
		namespace,
		name,
		value_type,
		id_type,
	})
}
