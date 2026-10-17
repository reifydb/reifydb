// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_core::{expression::Expression, row::JoinPick};
use reifydb_value::{
	Result,
	value::{
		dictionary::DictionaryId,
		value_type::{ValueType, field::from_field},
	},
};

pub(crate) fn kept_dictionary_columns(
	left: &Schema,
	right: &Schema,
	left_keys: &[Expression],
	right_keys: &[Expression],
	pick: Option<&JoinPick>,
) -> Result<(HashMap<String, DictionaryId>, HashMap<String, DictionaryId>)> {
	let mut left_kept = dictionary_columns(left)?;
	let mut right_kept = dictionary_columns(right)?;
	if let Some(pick) = pick {
		for key in &pick.keys {
			left_kept.remove(key.column.text());
			right_kept.remove(key.column.text());
		}
	}
	loop {
		let before = left_kept.len() + right_kept.len();
		for (left_key, right_key) in left_keys.iter().zip(right_keys) {
			let (left_name, right_name) = (bare(left_key), bare(right_key));
			let paired = match (left_name, right_name) {
				(Some(l), Some(r)) => {
					matches!((left_kept.get(l), right_kept.get(r)), (Some(a), Some(b)) if a == b)
				}
				_ => false,
			};
			if !paired {
				release(&mut left_kept, left_name);
				release(&mut right_kept, right_name);
			}
		}
		if left_kept.len() + right_kept.len() == before {
			return Ok((left_kept, right_kept));
		}
	}
}

pub(crate) fn entry_schema(raw: &Schema, decoded: &Schema, kept: &HashMap<String, DictionaryId>) -> SchemaRef {
	let fields: Vec<FieldRef> = raw
		.fields()
		.iter()
		.zip(decoded.fields())
		.map(|(raw, decoded)| match kept.contains_key(raw.name()) {
			true => raw.clone(),
			false => decoded.clone(),
		})
		.collect();
	Arc::new(Schema::new(fields))
}

pub(crate) fn dictionary_columns(schema: &Schema) -> Result<HashMap<String, DictionaryId>> {
	let mut columns = HashMap::new();
	for field in schema.fields() {
		let field_type = from_field(field)?;
		if let (Some(value_type), Some(dictionary)) = (&field_type.value_type, field_type.dictionary_id)
			&& *value_type.inner_type() == ValueType::DictionaryId
		{
			columns.insert(field.name().clone(), dictionary);
		}
	}
	Ok(columns)
}

fn bare(expression: &Expression) -> Option<&str> {
	match expression {
		Expression::Column(column) => Some(column.0.name.text()),
		_ => None,
	}
}

fn release(kept: &mut HashMap<String, DictionaryId>, name: Option<&str>) {
	match name {
		Some(name) => {
			kept.remove(name);
		}
		None => kept.clear(),
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use arrow_schema::Schema;
	use reifydb_core::{
		expression::Expression,
		row::JoinPick,
		sort::{SortDirection, SortKey},
		value::column::builder::ColumnBuilder,
	};
	use reifydb_rql::expression::parse_expression;
	use reifydb_value::{
		fragment::Fragment,
		value::{dictionary::DictionaryId, value_type::ValueType},
	};

	use super::kept_dictionary_columns;

	const SYMS: DictionaryId = DictionaryId(1);
	const OTHER: DictionaryId = DictionaryId(2);

	fn side(columns: &[(&str, Option<DictionaryId>)]) -> Schema {
		Schema::new(
			columns.iter()
				.map(|(name, dictionary)| {
					let value_type = match dictionary {
						Some(_) => ValueType::DictionaryId,
						None => ValueType::Utf8,
					};
					let mut builder = ColumnBuilder::with_capacity(value_type, 0);
					if let Some(dictionary) = dictionary {
						builder.set_dictionary_id(*dictionary);
					}
					builder.finish(name).0
				})
				.collect::<Vec<_>>(),
		)
	}

	fn kept(
		left: &[(&str, Option<DictionaryId>)],
		right: &[(&str, Option<DictionaryId>)],
		keys: (&str, &str),
		pick: Option<JoinPick>,
	) -> (Vec<String>, Vec<String>) {
		let parse = |keys: &str| -> Vec<Expression> {
			keys.split(", ").flat_map(|key| parse_expression(key).unwrap()).collect()
		};
		let (left_keys, right_keys) = (parse(keys.0), parse(keys.1));
		let (l, r) = kept_dictionary_columns(&side(left), &side(right), &left_keys, &right_keys, pick.as_ref())
			.unwrap();
		let names = |kept: HashMap<String, DictionaryId>| {
			let mut names: Vec<String> = kept.into_keys().collect();
			names.sort();
			names
		};
		(names(l), names(r))
	}

	fn owned(names: &[&str]) -> Vec<String> {
		names.iter().map(|name| name.to_string()).collect()
	}

	#[test]
	fn a_key_pair_of_one_dictionary_keeps_its_ids_and_payload_ids_always_stay() {
		// hashing equal ids of one dictionary matches exactly the rows equal text matches, so a decode is
		// wasted.
		let out = kept(
			&[("k", Some(SYMS)), ("p", Some(SYMS))],
			&[("k", Some(SYMS)), ("q", Some(OTHER))],
			("k", "k"),
			None,
		);
		assert_eq!(out, (owned(&["k", "p"]), owned(&["k", "q"])));
	}

	#[test]
	fn keys_that_cannot_hash_alike_are_decoded_on_both_sides() {
		// entry id 1 is a different mint in each dictionary, and an id never equals its text, so these keys
		// must meet as text.
		let two_dictionaries = kept(&[("k", Some(SYMS))], &[("k", Some(OTHER))], ("k", "k"), None);
		assert_eq!(two_dictionaries, (owned(&[]), owned(&[])));
		let id_against_text = kept(&[("k", Some(SYMS)), ("p", Some(SYMS))], &[("k", None)], ("k", "k"), None);
		assert_eq!(id_against_text, (owned(&["p"]), owned(&[])));
	}

	#[test]
	fn a_computed_key_decodes_every_id_column_of_its_side() {
		// the key expression may read any column, and a computed key over ids would hash a value no text row
		// produces.
		let out = kept(&[("k", Some(SYMS)), ("p", Some(SYMS))], &[("k", Some(SYMS))], ("k == p", "k"), None);
		assert_eq!(out, (owned(&[]), owned(&[])));
	}

	#[test]
	fn releasing_a_column_for_one_pair_releases_its_partner_in_every_other_pair() {
		// a kept partner of a decoded column would hash ids against text and never match again.
		let out = kept(&[("a", Some(SYMS))], &[("x", Some(SYMS)), ("y", None)], ("a, a", "x, y"), None);
		assert_eq!(out, (owned(&[]), owned(&[])));
	}

	#[test]
	fn a_pick_column_is_decoded_so_the_pick_orders_by_text() {
		// ids follow intern order, so a pick by id would choose the wrong latest row.
		let pick = JoinPick {
			keys: vec![SortKey {
				column: Fragment::internal("p"),
				direction: SortDirection::Desc,
			}],
		};
		let out = kept(&[("k", Some(SYMS))], &[("k", Some(SYMS)), ("p", Some(SYMS))], ("k", "k"), Some(pick));
		assert_eq!(out, (owned(&["k"]), owned(&["k"])));
	}
}
