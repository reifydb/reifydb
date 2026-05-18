// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::interface::catalog::binding::{BindingFormat, BindingProtocol, HttpMethod};

use super::common::{admin, fresh_db};

#[test]
fn create_binding_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create procedure demo::greet as { \"hi\" }");
	admin(
		&db,
		"create http binding demo::greet_http for demo::greet with { method: \"POST\", path: \"/demo/greet\", format: \"json\" }",
	);

	let cat = db.catalog();
	let mat = cat.cache();
	let ns = mat.find_namespace_by_name("demo").unwrap();
	let binding = mat.find_binding_by_name(ns.id(), "greet_http").unwrap();
	let proc = mat.find_procedure_by_name(ns.id(), "greet").unwrap();
	assert_eq!(binding.name, "greet_http");
	assert_eq!(binding.namespace, ns.id());
	assert_eq!(binding.procedure_id, proc.id());
	assert_eq!(binding.format, BindingFormat::Json);
	assert!(matches!(
		binding.protocol,
		BindingProtocol::Http {
			method: HttpMethod::Post,
			ref path,
		} if path == "/demo/greet"
	));
}
