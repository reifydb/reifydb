// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::json::{from::parse_value, wire_type::WireValueType};
use reifydb_value::{params::Params, value::Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WireValue {
	#[serde(rename = "type")]
	pub r#type: WireValueType,
	pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireParams {
	Positional(Vec<WireValue>),
	Named(HashMap<String, WireValue>),
}

fn wire_value_to_value(parameter: &str, wire: WireValue) -> Result<Value, String> {
	parse_value(&wire.r#type.0, &wire.value).map_err(|e| format!("parameter {parameter}: {e}"))
}

impl WireParams {
	pub fn into_params(self) -> Result<Params, String> {
		match self {
			WireParams::Positional(items) => {
				let mut values = Vec::with_capacity(items.len());
				for (index, item) in items.into_iter().enumerate() {
					values.push(wire_value_to_value(&format!("${}", index + 1), item)?);
				}
				Ok(Params::Positional(Arc::new(values)))
			}
			WireParams::Named(map) => {
				let mut result = HashMap::with_capacity(map.len());
				for (key, wire) in map {
					let value = wire_value_to_value(&format!("${key}"), wire)?;
					result.insert(key, value);
				}
				Ok(Params::Named(Arc::new(result)))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::json::{NONE_MARKER, none_marker};
	use reifydb_value::value::value_type::ValueType;
	use serde_json::from_str;

	use super::*;

	fn positional(json: &str) -> Result<Params, String> {
		from_str::<WireParams>(json).expect("wire params should deserialise").into_params()
	}

	fn named(json: &str) -> Result<Params, String> {
		from_str::<WireParams>(json).expect("wire params should deserialise").into_params()
	}

	fn option(inner: ValueType) -> ValueType {
		ValueType::Option(Box::new(inner))
	}

	fn option_option_int4(value: &str) -> String {
		format!(
			r#"{{"type":{{"id":"Option","underlying":{{"id":"Option","underlying":{{"id":"Int4"}}}}}},"value":"{value}"}}"#
		)
	}

	#[test]
	fn a_none_under_one_some_layer_of_option_option_int4_is_a_none_of_int4() {
		let params = positional(&format!("[{}]", option_option_int4(&none_marker(1)))).unwrap();
		assert_eq!(params.get_positional(0), Some(&Value::none_of(ValueType::Int4)));

		let params = named(&format!(r#"{{"v":{}}}"#, option_option_int4(&none_marker(1)))).unwrap();
		assert_eq!(params.get_named("v"), Some(&Value::none_of(ValueType::Int4)));
	}

	#[test]
	fn a_bare_marker_on_option_option_int4_is_a_none_of_option_int4() {
		let params = positional(&format!("[{}]", option_option_int4(NONE_MARKER))).unwrap();
		assert_eq!(params.get_positional(0), Some(&Value::none_of(option(ValueType::Int4))));

		let params = named(&format!(r#"{{"v":{}}}"#, option_option_int4(NONE_MARKER))).unwrap();
		assert_eq!(params.get_named("v"), Some(&Value::none_of(option(ValueType::Int4))));
	}

	#[test]
	fn a_present_value_on_option_option_int4_is_the_base_value() {
		let params = positional(&format!("[{}]", option_option_int4("5"))).unwrap();
		assert_eq!(params.get_positional(0), Some(&Value::Int4(5)));

		let params = named(&format!(r#"{{"v":{}}}"#, option_option_int4("5"))).unwrap();
		assert_eq!(params.get_named("v"), Some(&Value::Int4(5)));
	}

	#[test]
	fn a_scalar_descriptor_is_a_base_type() {
		let params = positional(r#"[{"type":{"id":"Int4"},"value":"7"}]"#).unwrap();
		assert_eq!(params.get_positional(0), Some(&Value::Int4(7)));
	}

	#[test]
	fn nested_descriptors_carry_the_option_layers() {
		let params = positional(
			r#"[{"type":{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Utf8"}}},"value":"⟪none:1⟫"},{"type":{"id":"Option","underlying":{"id":"Utf8"}},"value":"seven"}]"#,
		)
		.unwrap();
		assert_eq!(params.get_positional(0), Some(&Value::none_of(ValueType::Utf8)));
		assert_eq!(params.get_positional(1), Some(&Value::Utf8("seven".to_string())));
	}

	#[test]
	fn the_none_type_name_is_unknown() {
		let err = from_str::<WireValue>(r#"{"type":{"id":"None"},"value":"⟪none⟫"}"#).unwrap_err().to_string();
		assert!(err.contains("unknown type id `None`"), "{err}");
	}

	#[test]
	fn a_positional_error_names_the_position_and_the_type() {
		let err = positional(r#"[{"type":{"id":"Int4"},"value":"1"},{"type":{"id":"Int4"},"value":"abc"}]"#)
			.unwrap_err();
		assert_eq!(err, "parameter $2: invalid data: cannot parse 'abc' as Int4");
	}

	#[test]
	fn a_named_error_names_the_parameter_and_the_type() {
		let params = from_str::<WireParams>(r#"{"id":{"type":{"id":"Int4"},"value":"⟪none⟫"}}"#).unwrap();
		let err = params.into_params().unwrap_err();
		assert_eq!(err, "parameter $id: invalid data: none marker for non-Option type Int4");
	}
}
