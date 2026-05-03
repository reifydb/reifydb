// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::r#type::value::r#type::Type;

use super::common::{admin, fresh_db};

#[test]
fn create_dictionary_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create dictionary demo::d for utf8 as uint4");

	let mat = &db.engine().catalog().materialized;
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let dict = mat.find_dictionary_by_name(ns.id(), "d").unwrap();
	assert_eq!(dict.name, "d");
	assert_eq!(dict.namespace, ns.id());
	assert_eq!(dict.value_type, Type::Utf8);
	assert_eq!(dict.id_type, Type::Uint4);
}
