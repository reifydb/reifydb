// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	dictionary::Dictionary,
	id::{ColumnId, NamespaceId, RingBufferId, SeriesId, TableId},
	ringbuffer::RingBuffer,
	series::{Series, SeriesKey, TimestampPrecision},
	sumtype::{Field, SumType, SumTypeKind, Variant},
	table::Table,
};
use reifydb_export::{
	model::{NameResolver, ResolvedDictionary, ResolvedSumType, ResolvedVariant},
	render::ddl::{
		render_dictionary, render_enum, render_namespace, render_ringbuffer, render_series, render_table,
	},
};
use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes},
	dictionary::DictionaryId,
	sumtype::SumTypeId,
	value_type::ValueType,
};

const NS: u64 = 100;
const DICT: u64 = 7;
const SUM: u64 = 9;
const SUM2: u64 = 11;

fn resolver() -> NameResolver {
	let mut r = NameResolver::empty();
	r.namespaces.insert(NS, "sales".to_string());
	r.dictionaries.insert(
		DICT,
		ResolvedDictionary {
			qualified_name: "sales::tokens".to_string(),
			value_type: ValueType::Utf8,
		},
	);
	r.sumtypes.insert(
		SUM,
		ResolvedSumType {
			qualified_name: "sales::status".to_string(),
			variants: vec![
				ResolvedVariant {
					tag: 0,
					name: "active".to_string(),
					fields: vec![],
				},
				ResolvedVariant {
					tag: 1,
					name: "inactive".to_string(),
					fields: vec![],
				},
			],
		},
	);
	r.sumtypes.insert(
		SUM2,
		ResolvedSumType {
			qualified_name: "sales::shape".to_string(),
			variants: vec![
				ResolvedVariant {
					tag: 0,
					name: "circle".to_string(),
					fields: vec!["radius".to_string()],
				},
				ResolvedVariant {
					tag: 1,
					name: "rectangle".to_string(),
					fields: vec!["width".to_string(), "height".to_string()],
				},
			],
		},
	);
	r
}

fn column(id: u64, name: &str, constraint: TypeConstraint) -> Column {
	Column {
		id: ColumnId(id),
		name: name.to_string(),
		constraint,
		properties: vec![],
		index: ColumnIndex(id as u8),
		auto_increment: false,
		dictionary_id: None,
	}
}

fn table(name: &str, columns: Vec<Column>) -> Table {
	Table {
		id: TableId(1),
		namespace: NamespaceId(NS),
		name: name.to_string(),
		columns,
		primary_key: None,
		partition_by: vec![],
		underlying: false,
	}
}

#[test]
fn table_with_plain_and_constrained_columns() {
	let t = table(
		"orders",
		vec![
			column(0, "id", TypeConstraint::unconstrained(ValueType::Int4)),
			column(
				1,
				"name",
				TypeConstraint::with_constraint(
					ValueType::Utf8,
					Constraint::MaxBytes(MaxBytes::new(50)),
				),
			),
			column(2, "note", TypeConstraint::unconstrained(ValueType::Option(Box::new(ValueType::Utf8)))),
		],
	);
	assert_eq!(
		render_table(&t, &resolver(), false).unwrap(),
		"CREATE TABLE sales::orders { id: int4, name: utf8(50), note: option(utf8) };"
	);
	assert_eq!(
		render_table(&t, &resolver(), true).unwrap(),
		"CREATE TABLE IF NOT EXISTS sales::orders { id: int4, name: utf8(50), note: option(utf8) };"
	);
}

#[test]
fn table_with_dictionary_column_via_constraint() {
	let t = table("t", vec![column(0, "code", TypeConstraint::dictionary(DictionaryId(DICT), ValueType::Uint4))]);
	assert_eq!(
		render_table(&t, &resolver(), false).unwrap(),
		"CREATE TABLE sales::t { code: utf8 with { dictionary: sales::tokens } };"
	);
}

#[test]
fn table_with_dictionary_column_via_dictionary_id_field() {
	let mut col = column(0, "code", TypeConstraint::unconstrained(ValueType::Utf8));
	col.dictionary_id = Some(DictionaryId(DICT));
	assert_eq!(
		render_table(&table("t", vec![col]), &resolver(), false).unwrap(),
		"CREATE TABLE sales::t { code: utf8 with { dictionary: sales::tokens } };"
	);
}

#[test]
fn table_with_sumtype_column() {
	let t = table("t", vec![column(0, "state", TypeConstraint::sumtype(SumTypeId(SUM)))]);
	assert_eq!(render_table(&t, &resolver(), false).unwrap(), "CREATE TABLE sales::t { state: sales::status };");
}

#[test]
fn ring_buffer_with_partition_by() {
	let rb = RingBuffer {
		id: RingBufferId(1),
		namespace: NamespaceId(NS),
		name: "events".to_string(),
		columns: vec![
			column(0, "id", TypeConstraint::unconstrained(ValueType::Int4)),
			column(1, "region", TypeConstraint::unconstrained(ValueType::Utf8)),
		],
		capacity: 1000,
		primary_key: None,
		partition_by: vec!["region".to_string()],
		underlying: false,
	};
	assert_eq!(
		render_ringbuffer(&rb, &resolver()).unwrap(),
		"CREATE RINGBUFFER sales::events { id: int4, region: utf8 } WITH { capacity: 1000, partition: { by: { region } } };"
	);
}

#[test]
fn series_datetime_key_with_precision_and_tag() {
	let series = Series {
		id: SeriesId(1),
		namespace: NamespaceId(NS),
		name: "metrics".to_string(),
		columns: vec![
			column(0, "ts", TypeConstraint::unconstrained(ValueType::DateTime)),
			column(1, "value", TypeConstraint::unconstrained(ValueType::Int4)),
		],
		tag: Some(SumTypeId(SUM)),
		key: SeriesKey::DateTime {
			column: "ts".to_string(),
			precision: TimestampPrecision::Millisecond,
		},
		primary_key: None,
		partition_by: vec![],
		underlying: false,
	};
	assert_eq!(
		render_series(&series, &resolver()).unwrap(),
		"CREATE SERIES sales::metrics { ts: datetime, value: int4 } WITH { key: ts, tag: sales::status, precision: millisecond };"
	);
}

#[test]
fn series_integer_key_has_no_precision() {
	let series = Series {
		id: SeriesId(2),
		namespace: NamespaceId(NS),
		name: "seq".to_string(),
		columns: vec![
			column(0, "k", TypeConstraint::unconstrained(ValueType::Int8)),
			column(1, "value", TypeConstraint::unconstrained(ValueType::Int4)),
		],
		tag: None,
		key: SeriesKey::Integer {
			column: "k".to_string(),
		},
		primary_key: None,
		partition_by: vec![],
		underlying: false,
	};
	assert_eq!(
		render_series(&series, &resolver()).unwrap(),
		"CREATE SERIES sales::seq { k: int8, value: int4 } WITH { key: k };"
	);
}

#[test]
fn dictionary_definition() {
	let dict = Dictionary {
		id: DictionaryId(DICT),
		namespace: NamespaceId(NS),
		name: "tokens".to_string(),
		value_type: ValueType::Utf8,
		id_type: ValueType::Uint4,
	};
	assert_eq!(
		render_dictionary(&dict, &resolver(), false).unwrap(),
		"CREATE DICTIONARY sales::tokens FOR utf8 AS uint4;"
	);
}

#[test]
fn enum_plain_and_with_fields() {
	let plain = SumType {
		id: SumTypeId(SUM),
		namespace: NamespaceId(NS),
		name: "status".to_string(),
		variants: vec![
			Variant {
				tag: 1,
				name: "Inactive".to_string(),
				fields: vec![],
			},
			Variant {
				tag: 0,
				name: "Active".to_string(),
				fields: vec![],
			},
		],
		kind: SumTypeKind::Enum,
	};
	assert_eq!(render_enum(&plain, &resolver(), false).unwrap(), "CREATE ENUM sales::status { Active, Inactive };");

	let shaped = SumType {
		id: SumTypeId(SUM),
		namespace: NamespaceId(NS),
		name: "shape".to_string(),
		variants: vec![Variant {
			tag: 0,
			name: "Circle".to_string(),
			fields: vec![Field {
				name: "radius".to_string(),
				field_type: TypeConstraint::unconstrained(ValueType::Float8),
			}],
		}],
		kind: SumTypeKind::Enum,
	};
	assert_eq!(
		render_enum(&shaped, &resolver(), false).unwrap(),
		"CREATE ENUM sales::shape { Circle { radius: float8 } };"
	);
}

#[test]
fn namespace_definition() {
	let ns = reifydb_core::interface::catalog::namespace::Namespace::Local {
		id: NamespaceId(NS),
		name: "sales".to_string(),
		local_name: "sales".to_string(),
		parent_id: NamespaceId::ROOT,
	};
	assert_eq!(render_namespace(&ns, false), "CREATE NAMESPACE sales;");
	assert_eq!(render_namespace(&ns, true), "CREATE NAMESPACE IF NOT EXISTS sales;");
}

#[test]
fn enum_unit_column_collapses_tag_to_logical() {
	// An enum column is stored physically as `<col>_tag` (Uint1 + SumType constraint);
	// export must collapse it back to the logical `state: sales::status`.
	let t = table(
		"t",
		vec![
			column(0, "id", TypeConstraint::unconstrained(ValueType::Int4)),
			column(1, "state_tag", TypeConstraint::sumtype(SumTypeId(SUM))),
		],
	);
	assert_eq!(
		render_table(&t, &resolver(), false).unwrap(),
		"CREATE TABLE sales::t { id: int4, state: sales::status };"
	);
}

#[test]
fn enum_structured_column_absorbs_field_columns() {
	// A structured-variant enum column expands to a `_tag` column plus one Option column per
	// variant field (`shape_circle_radius`, ...); export must absorb the field columns and emit
	// only the single logical `shape: sales::shape`.
	let opt_f8 = || TypeConstraint::unconstrained(ValueType::Option(Box::new(ValueType::Float8)));
	let t = table(
		"t",
		vec![
			column(0, "id", TypeConstraint::unconstrained(ValueType::Int4)),
			column(1, "shape_tag", TypeConstraint::sumtype(SumTypeId(SUM2))),
			column(2, "shape_circle_radius", opt_f8()),
			column(3, "shape_rectangle_width", opt_f8()),
			column(4, "shape_rectangle_height", opt_f8()),
		],
	);
	assert_eq!(
		render_table(&t, &resolver(), false).unwrap(),
		"CREATE TABLE sales::t { id: int4, shape: sales::shape };"
	);
}
