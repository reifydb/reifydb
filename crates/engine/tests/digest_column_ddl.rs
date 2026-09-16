// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::cache::{CatalogCache, load::CatalogCacheLoader};
use reifydb_core::interface::catalog::{column::Column, id::NamespaceId};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{
		Value, constraint::TypeConstraint, digest::Digest, duration::Duration, frame::frame::Frame,
		identity::IdentityId, value_type::ValueType,
	},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t
}

fn admin(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
	}
}

fn create_table_err(column_type: &str) -> Diagnostic {
	let t = engine();
	let rql = format!("CREATE TABLE test::bad {{ c: {column_type} }}");
	admin(&t, &rql).expect_err(&format!("{rql} must fail"))
}

fn digest(inner: ValueType, accuracy: u32) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
	}
}

fn optional(inner: ValueType) -> ValueType {
	ValueType::Option(Box::new(inner))
}

fn test_namespace(catalog: &CatalogCache) -> NamespaceId {
	catalog.find_namespace_by_name("test").expect("namespace test is in the catalog").id()
}

fn table_types(catalog: &CatalogCache, table: &str) -> Vec<(String, ValueType)> {
	let table = catalog.find_table_by_name(test_namespace(catalog), table).expect("table is in the catalog");
	table.columns.iter().map(|c| (c.name.clone(), c.constraint.get_type())).collect()
}

fn column_type(columns: &[Column], name: &str) -> ValueType {
	columns.iter().find(|c| c.name == name).expect("column is in the catalog").constraint.get_type()
}

fn reloaded_catalog(t: &TestEngine) -> CatalogCache {
	let cold = CatalogCache::new();
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	CatalogCacheLoader::load_all(&mut Transaction::Command(&mut txn), &cold).unwrap();
	txn.rollback().unwrap();
	cold
}

const DIGEST_TABLE: &str = "CREATE TABLE test::latency { \
	service: utf8, \
	lat: digest(duration, 0.01), \
	opt: Option(digest(float8, 0.0125)), \
	padded: digest(int4, 0.010) \
}";

fn expected_digest_table() -> Vec<(String, ValueType)> {
	vec![
		("service".to_string(), ValueType::Utf8),
		("lat".to_string(), digest(ValueType::Duration, 10_000)),
		("opt".to_string(), optional(digest(ValueType::Float8, 12_500))),
		("padded".to_string(), digest(ValueType::Int4, 10_000)),
	]
}

#[test]
fn a_digest_column_type_reads_back_from_the_catalog() {
	// Inner type and accuracy are part of the type (D5); a bare digest or a rounded accuracy is another type.
	let t = engine();
	admin(&t, DIGEST_TABLE).expect("create table with digest columns");

	assert_eq!(table_types(t.catalog().cache(), "latency"), expected_digest_table());
	assert_eq!(digest(ValueType::Duration, 10_000).to_string(), "Digest(Duration, 0.01)");
}

#[test]
fn a_digest_column_type_survives_a_catalog_reload() {
	// A type tag byte cannot carry inner type and accuracy, so a reload reading only the tag loses them.
	let t = engine();
	admin(&t, DIGEST_TABLE).expect("create table with digest columns");

	let cold = reloaded_catalog(&t);

	assert_eq!(table_types(&cold, "latency"), expected_digest_table());
}

#[test]
fn an_unsupported_digest_input_type_is_rejected_at_the_input_type() {
	// A digest over a type with no natural zero has no meaningful relative error, so it must fail (D12).
	for (input, shown) in [
		("utf8", "Utf8"),
		("datetime", "DateTime"),
		("date", "Date"),
		("time", "Time"),
		("boolean", "Boolean"),
		("uuid7", "Uuid7"),
		("blob", "Blob"),
		("identity_id", "IdentityId"),
		("decimal", "Decimal"),
	] {
		let err = create_table_err(&format!("digest({input}, 0.01)"));

		assert_eq!(err.code, "AST_013", "digest({input}, 0.01): {err:?}");
		assert_eq!(err.message, format!("digest not supported for {shown}"));
		assert_eq!(err.fragment.text(), input, "the error must point at the input type");
	}
}

#[test]
fn a_wrapped_digest_input_type_is_rejected() {
	// Option and digest inputs are not values a digest can count, so they must fail like any unsupported input.
	let err = create_table_err("digest(Option(float8), 0.01)");
	assert_eq!(err.code, "AST_013", "{err:?}");
	assert_eq!(err.message, "digest not supported for Option(Float8)");

	let err = create_table_err("digest(digest(float8, 0.01), 0.01)");
	assert_eq!(err.code, "AST_013", "{err:?}");
	assert_eq!(err.message, "digest not supported for Digest(Float8, 0.01)");
}

#[test]
fn a_constrained_digest_input_type_is_rejected() {
	// Accepting int(8) as the input would silently drop its byte limit from the column type.
	let err = create_table_err("digest(int(8), 0.01)");

	assert_eq!(err.code, "AST_012", "{err:?}");
	assert_eq!(err.fragment.text(), "int");
}

#[test]
fn an_accuracy_outside_the_range_is_rejected_at_the_accuracy() {
	// An accuracy outside 0.001 to 0.1 must never reach the catalog, where no digest could be built for it (D20).
	for accuracy in ["0.5", "0.0009", "0", "1", "0.100001"] {
		let err = create_table_err(&format!("digest(float8, {accuracy})"));

		assert_eq!(err.code, "AST_014", "accuracy {accuracy}: {err:?}");
		assert_eq!(err.message, "accuracy must be between 0.001 and 0.1");
		assert_eq!(err.fragment.text(), accuracy, "the error must point at the accuracy");
	}
}

#[test]
fn an_accuracy_that_is_not_a_whole_ppm_is_rejected_at_the_accuracy() {
	// Rounding 0.0100005 to a whole ppm would store an accuracy nobody wrote (D46).
	let err = create_table_err("digest(float8, 0.0100005)");

	assert_eq!(err.code, "AST_015", "{err:?}");
	assert_eq!(err.message, "accuracy must be a whole number of parts per million");
	assert_eq!(err.fragment.text(), "0.0100005");
}

#[test]
fn an_accuracy_that_is_not_a_number_is_rejected_at_the_accuracy() {
	// A quoted, hex or type accuracy must not be read as a number the user never wrote.
	for (accuracy, fragment) in [("0x10", "0x10"), ("float8", "float8"), ("true", "true"), ("'0.01'", "0.01")] {
		let err = create_table_err(&format!("digest(float8, {accuracy})"));

		assert_eq!(err.code, "AST_016", "accuracy {accuracy}: {err:?}");
		assert_eq!(err.message, "accuracy must be a number");
		assert_eq!(err.fragment.text(), fragment, "the error must point at the accuracy");
	}
}

#[test]
fn a_digest_without_an_accuracy_is_rejected() {
	// A digest column with a default accuracy would contradict D4: accuracy is always written, never assumed.
	let err = create_table_err("digest(float8)");
	assert_eq!(err.code, "AST_017", "{err:?}");
	assert_eq!(err.message, "digest needs an input type and an accuracy");
	assert_eq!(err.fragment.text(), "float8");

	let err = create_table_err("digest");
	assert_eq!(err.code, "AST_017", "{err:?}");
	assert_eq!(err.fragment.text(), "digest");
}

#[test]
fn a_digest_with_an_extra_parameter_is_rejected_at_the_extra_parameter() {
	// Ignoring a third parameter would accept a type the user did not mean.
	let err = create_table_err("digest(float8, 0.01, 3)");

	assert_eq!(err.code, "AST_018", "{err:?}");
	assert_eq!(err.message, "digest takes an input type and an accuracy only");
	assert_eq!(err.fragment.text(), "3");
}

#[test]
fn a_digest_input_that_is_not_a_type_is_rejected() {
	// Swapped parameters must fail at the input, not be read as an accuracy.
	let err = create_table_err("digest(5, 0.01)");

	assert_eq!(err.code, "AST_019", "{err:?}");
	assert_eq!(err.message, "digest input must be a type");
	assert_eq!(err.fragment.text(), "5");
}

#[test]
fn a_type_parameter_on_a_non_digest_type_is_rejected() {
	// A type accepted as a parameter must not slip through types that only take numbers.
	let err = create_table_err("utf8(int4)");

	assert_eq!(err.code, "AST_012", "{err:?}");
	assert_eq!(err.fragment.text(), "utf8");
}

#[test]
fn every_column_ddl_form_keeps_a_digest_column_type_across_a_reload() {
	// Each object kind stores columns on its own path; any that drops inner type or accuracy changes the type.
	let t = engine();
	for rql in [
		"CREATE TABLE test::src { lat: digest(float8, 0.01) }",
		"ALTER TABLE test::src ADD COLUMN opt: Option(digest(uint2, 0.001))",
		"CREATE DEFERRED VIEW test::dv { lat: digest(float8, 0.01) } AS { FROM test::src }",
		"CREATE RINGBUFFER test::rb { lat: digest(int8, 0.05) } WITH { capacity: 10 }",
		"CREATE SERIES test::s { ts: int8, lat: digest(duration, 0.1) } WITH { key: ts }",
		"CREATE QUEUE test::q { lat: digest(float4, 0.02) } WITH { fifo: {} }",
	] {
		admin(&t, rql).unwrap_or_else(|e| panic!("{rql}: {e:?}"));
	}

	let cold = reloaded_catalog(&t);

	for catalog in [t.catalog().cache(), &cold] {
		let ns = test_namespace(catalog);
		let table = catalog.find_table_by_name(ns, "src").unwrap();
		assert_eq!(column_type(&table.columns, "lat"), digest(ValueType::Float8, 10_000));
		assert_eq!(column_type(&table.columns, "opt"), optional(digest(ValueType::Uint2, 1_000)));
		let view = catalog.find_view_by_name(ns, "dv").unwrap();
		assert_eq!(column_type(view.columns(), "lat"), digest(ValueType::Float8, 10_000));
		let ringbuffer = catalog.find_ringbuffer_by_name(ns, "rb").unwrap();
		assert_eq!(column_type(&ringbuffer.columns, "lat"), digest(ValueType::Int8, 50_000));
		let series = catalog.find_series_by_name(ns, "s").unwrap();
		assert_eq!(column_type(&series.columns, "lat"), digest(ValueType::Duration, 100_000));
		let queue = catalog.find_queue_by_name(ns, "q").unwrap();
		assert_eq!(column_type(&queue.columns, "lat"), digest(ValueType::Float4, 20_000));
	}
}

#[test]
fn procedure_parameters_and_variant_fields_keep_a_digest_type_across_a_reload() {
	// Procedure params and sum type fields persist types outside the column store and must round trip too.
	let t = engine();
	for rql in [
		"CREATE PROCEDURE test::p { d: digest(float8, 0.01) } AS { map { n: 1 } }",
		"CREATE ENUM test::e { A { d: digest(int4, 0.02) } }",
		"CREATE EVENT test::ev { Happened { d: digest(uint8, 0.03) } }",
		"CREATE TAG test::tg { Marked { d: Option(digest(duration, 0.04)) } }",
	] {
		admin(&t, rql).unwrap_or_else(|e| panic!("{rql}: {e:?}"));
	}

	let cold = reloaded_catalog(&t);

	for catalog in [t.catalog().cache(), &cold] {
		let ns = test_namespace(catalog);
		let procedure = catalog.find_procedure_by_name(ns, "p").unwrap();
		assert_eq!(
			procedure.params()[0].param_type,
			TypeConstraint::unconstrained(digest(ValueType::Float8, 10_000))
		);
		for (sumtype, expected) in [
			("e", digest(ValueType::Int4, 20_000)),
			("ev", digest(ValueType::Uint8, 30_000)),
			("tg", optional(digest(ValueType::Duration, 40_000))),
		] {
			let found = catalog.find_sumtype_by_name(ns, sumtype).unwrap();
			assert_eq!(found.variants[0].fields[0].field_type, TypeConstraint::unconstrained(expected));
		}
	}
}

#[test]
fn a_function_parameter_accepts_a_digest_type() {
	// Function parameter annotations have their own parse entry point, which must accept a type parameter.
	let t = engine();
	let r = t.inner().query_as(
		TestEngine::identity(),
		"UDF keep ($d: digest(float8, 0.01)) { RETURN 1 }; MAP { x: 1 }",
		Params::None,
	);

	assert!(r.error.is_none(), "{:?}", r.error.map(|e| e.diagnostic()));
}

#[test]
fn a_dictionary_over_a_digest_type_is_rejected_at_the_type() {
	// A dictionary stores its types as a bare tag byte, so accepting a digest panics every later catalog read.
	for (rql, shown) in [
		("CREATE DICTIONARY test::d FOR digest(float8, 0.01) AS uint4", "Digest(Float8, 0.01)"),
		("CREATE DICTIONARY test::d FOR Option(digest(float8, 0.01)) AS uint4", "Option(Digest(Float8, 0.01))"),
		("CREATE DICTIONARY test::d FOR utf8 AS digest(uint4, 0.01)", "Digest(Uint4, 0.01)"),
	] {
		let t = engine();
		let err = admin(&t, rql).expect_err(rql);

		assert_eq!(err.code, "CA_099", "{rql}: {err:?}");
		assert_eq!(err.message, format!("dictionary `d` does not support type `{shown}`"));
		assert_eq!(err.fragment.text(), "digest", "the error must point at the type");
		reloaded_catalog(&t);
	}
}

#[test]
fn a_user_attribute_of_a_digest_type_is_rejected() {
	// A user attribute persists its type as a bare tag, so accepting a digest panics the next catalog load.
	let t = engine();
	let err = admin(&t, "CREATE USER ATTRIBUTE lat: digest(float8, 0.01)").expect_err("digest user attribute");

	assert_eq!(err.code, "CA_092", "{err:?}");
	assert_eq!(err.message, "user attribute `lat` has unsupported type `Digest(Float8, 0.01)`");
	reloaded_catalog(&t);
}

#[test]
fn a_digest_primary_key_error_names_the_digest() {
	// A key over a digest must fail naming the digest, never blame UTF8 or BLOB columns the key does not have
	// (D21).
	let t = engine();
	admin(&t, DIGEST_TABLE).expect("create table with digest columns");
	let mut lat = Digest::new(ValueType::Duration, 10_000).unwrap();
	lat.add_value(&Value::Duration(Duration::from_milliseconds(5).unwrap())).unwrap();
	let padded = Digest::new(ValueType::Int4, 10_000).unwrap();
	let params = Params::from(HashMap::from([
		("lat".to_string(), Value::Digest(Box::new(lat))),
		("padded".to_string(), Value::Digest(Box::new(padded))),
	]));

	let err = match admin(&t, "CREATE PRIMARY KEY ON test::latency { lat }") {
		Err(err) => err,
		Ok(_) => {
			let insert = "INSERT test::latency [{ service: 'a', lat: $lat, opt: none, padded: $padded }]";
			let r = t.inner().admin_as(TestEngine::identity(), insert, params);
			r.error.expect("a table keyed by a digest must not take a row").diagnostic()
		}
	};

	assert!(err.message.to_lowercase().contains("digest"), "{err:?}");
}
