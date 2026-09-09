// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{frame::frame::Frame, value_type::ValueType};
use serde_json::{Error, Value as JsonValue, from_str, from_value, to_string};

use crate::json::{from::frames_from_json, wire_type::to_json};

pub fn frames_from_fixture_json(json: &str) -> Result<Vec<Frame>, Error> {
	let mut frames: JsonValue = from_str(json)?;
	rewrite_types(&mut frames)?;
	frames_from_json(&to_string(&frames)?)
}

fn rewrite_types(frames: &mut JsonValue) -> Result<(), Error> {
	let Some(frames) = frames.as_array_mut() else {
		return Ok(());
	};
	for frame in frames {
		let Some(columns) = frame.get_mut("columns").and_then(JsonValue::as_array_mut) else {
			continue;
		};
		for column in columns {
			let Some(value) = column.get_mut("type") else {
				continue;
			};
			let parsed: ValueType = from_value(value.clone())?;
			*value = to_json(&parsed);
		}
	}
	Ok(())
}
