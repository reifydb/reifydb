// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

pub mod join;
pub mod ping;
pub mod read;
pub mod scan;
pub mod write;

pub const NAMESPACE: &str = "bench";
pub const USERS_COLUMNS: &[&str] = &["id", "name", "email"];

pub fn create_namespace() -> String {
	format!("create namespace if not exists {}", NAMESPACE)
}

pub fn drop_namespace() -> String {
	format!("drop namespace {}", NAMESPACE)
}

pub fn create_users() -> String {
	format!("create table {}::users {{ id: int8, name: utf8, email: utf8 }}", NAMESPACE)
}

pub fn user_row(index: u64, _scale: u64) -> Vec<Value> {
	vec![
		Value::Int8(index as i64),
		Value::Utf8(format!("user_{}", index)),
		Value::Utf8(format!("user_{}@bench.test", index)),
	]
}
