// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Value types for expression evaluation.

use std::collections::HashMap;

use reifydb_type::Value;

/// Value types that can be used in expression evaluation.
#[derive(Debug, Clone)]
pub enum EvalValue {
	/// A scalar value.
	Scalar(Value),
	/// A record (map of field names to values).
	Record(HashMap<String, Value>),
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_eval_value_record() {
		let mut fields = HashMap::new();
		fields.insert("x".to_string(), Value::Int8(10));
		fields.insert("y".to_string(), Value::Int8(20));

		let record = EvalValue::Record(fields);

		if let EvalValue::Record(map) = record {
			assert_eq!(map.get("x"), Some(&Value::Int8(10)));
			assert_eq!(map.get("y"), Some(&Value::Int8(20)));
		} else {
			panic!("expected Record variant");
		}
	}
}
