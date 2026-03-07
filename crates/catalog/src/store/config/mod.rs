// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::store::MultiVersionValues,
	key::{EncodableKey, config::ConfigKey},
};
use reifydb_type::value::Value;

use crate::store::config::schema::config::{SCHEMA, VALUE};

pub mod get;
pub mod schema;
pub mod set;

pub(crate) fn convert_config(multi: MultiVersionValues) -> (String, Value) {
	let config_key = ConfigKey::decode(&multi.key).map(|k| k.key).unwrap_or_default();

	let value = match SCHEMA.get_value(&multi.values, VALUE) {
		Value::Any(inner) => *inner,
		other => other,
	};

	(config_key, value)
}
