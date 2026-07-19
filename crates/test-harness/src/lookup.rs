// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::identity::Identity;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, identity::IdentityId};

use crate::engine::AsEngine;

pub fn find_identity_by_attribute(engine: &impl AsEngine, attribute_name: &str, value: &Value) -> Option<Identity> {
	let engine = engine.standard_engine();
	let mut txn = engine.begin_query(IdentityId::root()).unwrap();
	engine
		.catalog()
		.find_identity_by_attribute_value(&mut Transaction::Query(&mut txn), attribute_name, value)
		.unwrap()
}

pub fn identity_attribute(engine: &impl AsEngine, identity: IdentityId, name: &str) -> Option<Value> {
	let engine = engine.standard_engine();
	let mut txn = engine.begin_query(IdentityId::root()).unwrap();
	let catalog = engine.catalog();
	let attribute = catalog.find_identity_attribute_by_name(&mut Transaction::Query(&mut txn), name).unwrap()?;
	catalog
		.find_identity_attribute_values(&mut Transaction::Query(&mut txn), identity)
		.unwrap()
		.into_iter()
		.find(|value| value.attribute == attribute.id)
		.map(|value| value.value)
}
