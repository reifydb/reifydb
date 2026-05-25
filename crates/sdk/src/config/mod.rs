// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_type::value::{
	Value,
	try_from::{TryFromValue, TryFromValueCoerce},
};

pub mod bool;
pub mod date;
pub mod datetime;
pub mod decimal;
pub mod dictionary;
pub mod duration;
pub mod f32;
pub mod f64;
pub mod i128;
pub mod i16;
pub mod i32;
pub mod i64;
pub mod i8;
pub mod identity;
pub mod int;
pub mod string;
pub mod time;
pub mod u128;
pub mod u16;
pub mod u32;
pub mod u64;
pub mod u8;
pub mod uint;
pub mod usize;

#[derive(Debug, Clone)]
pub struct Config {
	name: String,
	values: BTreeMap<String, Value>,
}

impl Config {
	pub fn new(name: impl Into<String>, values: BTreeMap<String, Value>) -> Self {
		Self {
			name: name.into(),
			values,
		}
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn get(&self, key: &str) -> Option<&Value> {
		self.values.get(key)
	}

	pub fn contains(&self, key: &str) -> bool {
		self.values.contains_key(key)
	}

	fn opt<T: TryFromValue>(&self, key: &str) -> Option<T> {
		self.values.get(key).and_then(T::from_value)
	}

	fn opt_coerce<T: TryFromValueCoerce>(&self, key: &str) -> Option<T> {
		self.values.get(key).and_then(T::from_value_coerce)
	}

	fn missing(&self, key: &str, expected: &str) -> ! {
		panic!("{}: required config '{}' is missing or not {}", self.name, key, expected)
	}
}

#[cfg(test)]
pub(super) mod testutil {
	use std::collections::BTreeMap;

	use reifydb_type::value::Value;

	use super::Config;

	pub fn config(pairs: Vec<(&str, Value)>) -> Config {
		let values: BTreeMap<String, Value> = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
		Config::new("test_op", values)
	}
}
