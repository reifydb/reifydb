// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{catalog::config::ConfigKey, store::MultiVersionRow},
	key::{EncodableKey, config::ConfigStorageKey},
};
use reifydb_type::value::Value;

use crate::store::config::shape::config::{SHAPE, VALUE};

pub mod set;
pub mod shape;

pub(crate) fn convert_config(multi: MultiVersionRow) -> (ConfigKey, Value) {
	let config_key = ConfigStorageKey::decode(&multi.key)
		.map(|k| k.key)
		.unwrap_or_else(|| panic!("failed to decode ConfigStorageKey"));

	let value = match SHAPE.get_value(&multi.row, VALUE) {
		Value::Any(inner) => *inner,
		other => other,
	};

	(config_key, value)
}
