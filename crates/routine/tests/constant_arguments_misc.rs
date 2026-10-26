// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::{
	flow::operator::OperatorDef,
	value::column::{
		builder::ColumnBuilder,
		factory::{from_many, from_one},
	},
};
use reifydb_routine::function::{
	blob::{b58::BlobB58, b64::BlobB64, b64url::BlobB64url, hex::BlobHex, utf8::BlobUtf8},
	crypto::sha256::CryptoSha256,
	flow::to_json::OperatorDefToJson,
	is::{none::IsNone, some::IsSome, r#type::IsType},
	json::{array::JsonArray, object::JsonObject, pretty::JsonPretty, serialize::JsonSerialize},
	meta::r#type::Type,
	rql::fingerprint::RqlFingerprint,
	stats::approx_percentile::ApproxPercentile,
	uuid::{v4::UuidV4, v7::UuidV7},
};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, blob::Blob, digest::Digest, identity::IdentityId, value_type::ValueType},
};

const ROWS: usize = 4;

enum Arg {
	Constant(Value),
	Rows(ValueType, Vec<Value>),
}

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("misc"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn rows(ty: &ValueType, values: &[Value]) -> (FieldRef, ArrayRef) {
	let mut builder = ColumnBuilder::with_capacity(ty.clone(), values.len());
	for value in values {
		match value {
			Value::None {
				..
			} => builder.push_none(),
			value => builder.push_value(value.clone()),
		}
	}
	builder.finish("arg")
}

fn build(args: &[Arg], one_row_at: Option<usize>) -> Vec<(FieldRef, ArrayRef)> {
	args.iter()
		.enumerate()
		.map(|(index, arg)| match arg {
			Arg::Constant(value) if one_row_at == Some(index) => from_one("arg", value.clone()),
			Arg::Constant(value) => from_many("arg", value.clone(), ROWS),
			Arg::Rows(ty, values) => rows(ty, values),
		})
		.collect()
}

fn check(name: &str, function: impl Function, args: Vec<Arg>) {
	// The 1-row form of each constant must answer exactly what the old copied-to-every-row form answered.
	let repeated = function.call(&mut ctx(ROWS), &build(&args, None)).unwrap();
	assert_eq!(repeated.1.len(), ROWS, "{name}: the repeated form must answer {ROWS} rows");
	let constants: Vec<usize> =
		args.iter().enumerate().filter(|(_, arg)| matches!(arg, Arg::Constant(_))).map(|(i, _)| i).collect();
	assert!(!constants.is_empty(), "{name}: a case without a constant argument checks nothing");
	for index in constants {
		let one_row = function.call(&mut ctx(ROWS), &build(&args, Some(index))).unwrap();
		assert_eq!(one_row.1.len(), ROWS, "{name}: argument {index} as 1 row must still answer {ROWS} rows");
		assert_eq!(
			one_row.1.to_data(),
			repeated.1.to_data(),
			"{name}: argument {index} as 1 row answered {:?}, repeated answered {:?}",
			one_row.1,
			repeated.1
		);
	}
}

fn utf8(text: &str) -> Value {
	Value::Utf8(text.to_string())
}

fn none_of(inner: ValueType) -> Value {
	Value::None {
		inner,
	}
}

fn utf8_rows(texts: &[&str]) -> Arg {
	Arg::Rows(ValueType::Utf8, texts.iter().map(|text| utf8(text)).collect())
}

fn int4_rows() -> Arg {
	Arg::Rows(ValueType::Int4, (1..=ROWS as i32).map(Value::Int4).collect())
}

fn digest(values: &[f64]) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	digest
}

#[test]
fn blob_decoders_broadcast_a_constant_text() {
	// Otherwise a 1-row text decodes into a 1-row blob and the call answers fewer rows than the batch.
	check("blob::hex", BlobHex::new(), vec![Arg::Constant(utf8("cafe"))]);
	check("blob::b58", BlobB58::new(), vec![Arg::Constant(utf8("Cn8eVZg"))]);
	check("blob::b64", BlobB64::new(), vec![Arg::Constant(utf8("aGVsbG8="))]);
	check("blob::b64url", BlobB64url::new(), vec![Arg::Constant(utf8("aGVsbG8"))]);
	check("blob::utf8", BlobUtf8::new(), vec![Arg::Constant(utf8("hello"))]);
}

#[test]
fn blob_decoders_broadcast_a_none_constant() {
	// A 1-row none constant must make every row none, not just the first.
	check("blob::hex", BlobHex::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
	check("blob::utf8", BlobUtf8::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
}

#[test]
fn json_array_reads_each_constant_at_every_row() {
	// A constant item must land in every row's list, or rows after the first lose it.
	check(
		"json::array",
		JsonArray::new(),
		vec![
			Arg::Constant(Value::Int4(7)),
			utf8_rows(&["a", "b", "c", "d"]),
			Arg::Constant(none_of(ValueType::Int4)),
		],
	);
}

#[test]
fn json_object_reads_constant_keys_and_values_at_every_row() {
	// Constant keys and values must appear in every row's record, not only in row 0.
	check(
		"json::object",
		JsonObject::new(),
		vec![
			Arg::Constant(utf8("k")),
			int4_rows(),
			Arg::Constant(utf8("name")),
			Arg::Constant(utf8("v")),
			Arg::Constant(utf8("gone")),
			Arg::Constant(none_of(ValueType::Utf8)),
		],
	);
}

#[test]
fn json_text_functions_broadcast_a_constant() {
	// Otherwise a 1-row constant serializes into one string for a batch of many rows.
	check("json::serialize", JsonSerialize::new(), vec![Arg::Constant(utf8("x"))]);
	check("json::serialize", JsonSerialize::new(), vec![Arg::Constant(none_of(ValueType::Int4))]);
	check("json::pretty", JsonPretty::new(), vec![Arg::Constant(Value::Int4(3))]);
	check("json::pretty", JsonPretty::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
}

#[test]
fn uuid_parsers_broadcast_a_constant_text() {
	// A 1-row uuid text must parse into every row, and a none constant must leave every row none.
	check("uuid::v4", UuidV4::new(), vec![Arg::Constant(utf8("550e8400-e29b-41d4-a716-446655440000"))]);
	check("uuid::v4", UuidV4::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
	check("uuid::v7", UuidV7::new(), vec![Arg::Constant(utf8("01890a5d-ac96-774b-bcce-b302099a8057"))]);
	check("uuid::v7", UuidV7::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
}

#[test]
fn is_none_and_is_some_broadcast_a_constant() {
	// These read nulls without option propagation, so a 1-row constant must still answer every row.
	for value in [Value::Int4(1), none_of(ValueType::Int4), none_of(ValueType::Any)] {
		check("is::none", IsNone::new(), vec![Arg::Constant(value.clone())]);
		check("is::some", IsSome::new(), vec![Arg::Constant(value)]);
	}
}

#[test]
fn is_type_reads_a_constant_value_at_every_row() {
	// Otherwise the value's type is read past the end of a 1-row column for every row after the first.
	let target = Value::Any(Box::new(Value::Type(ValueType::Int4)));
	check("is::type", IsType::new(), vec![int4_rows(), Arg::Constant(target.clone())]);
	check("is::type", IsType::new(), vec![Arg::Constant(Value::Int4(1)), Arg::Constant(target.clone())]);
	check("is::type", IsType::new(), vec![Arg::Constant(none_of(ValueType::Int4)), Arg::Constant(target)]);
}

#[test]
fn meta_type_names_every_row_of_a_constant() {
	// The type name is repeated per row, so a 1-row constant must not shrink the answer to one row.
	check("meta::type", Type::new(), vec![Arg::Constant(Value::Int4(1))]);
	check("meta::type", Type::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
}

#[test]
fn sha256_hashes_a_constant_into_every_row() {
	// Without broadcasting, rows past a 1-row constant fall into the empty-string branch.
	check("crypto::sha256", CryptoSha256::new(), vec![Arg::Constant(utf8("abc"))]);
	check("crypto::sha256", CryptoSha256::new(), vec![Arg::Constant(none_of(ValueType::Utf8))]);
}

#[test]
fn rql_fingerprint_broadcasts_a_constant_query() {
	// A constant query must fingerprint into every row, not only the first.
	check("rql::fingerprint", RqlFingerprint::new(), vec![Arg::Constant(utf8("from test::t"))]);
}

#[test]
fn flow_node_to_json_broadcasts_a_constant_blob() {
	// Rows past a 1-row constant blob must render the same json, not an empty string.
	let bytes = postcard::to_stdvec(&OperatorDef::SourceInlineData {}).unwrap();
	check("flow_node::to_json", OperatorDefToJson::new(), vec![Arg::Constant(Value::Blob(Blob::new(bytes)))]);
}

#[test]
fn approx_percentile_broadcasts_a_constant_percentile_and_digest() {
	// A constant p must apply to every digest row, and a constant digest must answer every p row.
	let digests = Arg::Rows(
		ValueType::Digest {
			inner: Box::new(ValueType::Float8),
			accuracy: 10_000,
		},
		(1..=ROWS).map(|n| Value::Digest(Box::new(digest(&[1.0, 10.0 * n as f64])))).collect(),
	);
	check("stats::approx_percentile", ApproxPercentile::new(), vec![digests, Arg::Constant(Value::float8(0.9))]);
	check(
		"stats::approx_percentile",
		ApproxPercentile::new(),
		vec![
			Arg::Constant(Value::Digest(Box::new(digest(&[1.0, 2.0, 3.0, 4.0])))),
			Arg::Rows(ValueType::Float8, [0.0, 0.25, 0.5, 1.0].map(Value::float8).to_vec()),
		],
	);
	check(
		"stats::approx_percentile",
		ApproxPercentile::new(),
		vec![
			Arg::Constant(Value::Digest(Box::new(digest(&[1.0, 2.0])))),
			Arg::Constant(none_of(ValueType::Float8)),
		],
	);
}
