// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::dictionary::DictionaryDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{Value, dictionary::DictionaryEntryId};

use crate::transaction::operation::dictionary::DictionaryOperations;

pub(crate) fn decode_dictionary_columns(
	columns: &mut Columns,
	dictionaries: &[Option<DictionaryDef>],
	rx: &mut Transaction,
) -> crate::Result<()> {
	for (col_idx, dict_opt) in dictionaries.iter().enumerate() {
		if let Some(dictionary) = dict_opt {
			if col_idx >= columns.len() {
				continue;
			}
			let col = &columns[col_idx];
			let row_count = col.data().len();
			let mut new_data = ColumnData::with_capacity(dictionary.value_type.clone(), row_count);
			for row_idx in 0..row_count {
				let id_value = col.data().get_value(row_idx);
				if let Some(entry_id) = DictionaryEntryId::from_value(&id_value) {
					match rx.get_from_dictionary(dictionary, entry_id)? {
						Some(decoded) => new_data.push_value(decoded),
						None => new_data.push_value(Value::none()),
					}
				} else {
					new_data.push_value(Value::none());
				}
			}
			columns.columns.make_mut()[col_idx] = Column {
				name: columns[col_idx].name().clone(),
				data: new_data,
			};
		}
	}
	Ok(())
}

pub mod aggregate;
pub mod apply_transform;
pub mod assert;
pub mod compile;
pub mod distinct;
pub mod environment;
pub mod extend;
pub mod filter;
pub mod generator;
pub mod inline;
pub mod join;
pub mod map;
pub mod patch;
pub mod query;
pub mod row_lookup;
pub mod scalarize;
pub mod scan;
pub mod sort;
pub mod take;
pub mod top_k;
pub mod variable;
