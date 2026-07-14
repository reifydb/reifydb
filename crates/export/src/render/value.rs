// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

use crate::error::RenderError;

pub fn render_value(value: &Value) -> Result<String, RenderError> {
	match value {
		Value::None {
			..
		} => Ok("none".to_string()),
		Value::Boolean(b) => Ok(if *b {
			"true"
		} else {
			"false"
		}
		.to_string()),
		Value::Float4(f) => {
			if f.value().is_finite() {
				Ok(f.to_string())
			} else {
				Err(RenderError::NonFiniteFloat)
			}
		}
		Value::Float8(f) => {
			if f.value().is_finite() {
				Ok(f.to_string())
			} else {
				Err(RenderError::NonFiniteFloat)
			}
		}
		Value::Int1(v) => Ok(v.to_string()),
		Value::Int2(v) => Ok(v.to_string()),
		Value::Int4(v) => Ok(v.to_string()),
		Value::Int8(v) => Ok(v.to_string()),
		Value::Int16(v) => Ok(v.to_string()),
		Value::Uint1(v) => Ok(v.to_string()),
		Value::Uint2(v) => Ok(v.to_string()),
		Value::Uint4(v) => Ok(v.to_string()),
		Value::Uint8(v) => Ok(v.to_string()),
		Value::Uint16(v) => Ok(v.to_string()),
		Value::Int(v) => Ok(v.to_string()),
		Value::Uint(v) => Ok(v.to_string()),
		Value::Decimal(d) => Ok(format!("'{}'", d)),
		Value::Utf8(s) => render_text(s),
		Value::Date(d) => Ok(format!("@{}", d)),
		Value::Time(t) => Ok(format!("@{}", t)),
		Value::DateTime(dt) => Ok(format!("@{}", dt)),
		Value::Duration(dur) => Ok(format!("@{}", dur.to_iso_string())),
		Value::Blob(b) => {
			let display = b.to_string();
			let hex = display.strip_prefix("0x").unwrap_or(&display);
			Ok(format!("blob::hex('{}')", hex))
		}

		Value::Vector(v) => Ok(v.to_string()),
		Value::Uuid4(u) => Ok(format!("'{}'", u)),
		Value::Uuid7(u) => Ok(format!("'{}'", u)),
		Value::IdentityId(id) => Ok(format!("'{}'", id)),
		Value::DictionaryId(_) => Err(RenderError::Unsupported("DictionaryId")),
		Value::Any(_) => Err(RenderError::Unsupported("Any")),
		Value::Type(_) => Err(RenderError::Unsupported("Type")),
		Value::List(_) => Err(RenderError::Unsupported("List")),
		Value::Record(_) => Err(RenderError::Unsupported("Record")),
		Value::Tuple(_) => Err(RenderError::Unsupported("Tuple")),
	}
}

pub fn render_text(s: &str) -> Result<String, RenderError> {
	let has_single = s.contains('\'');
	let has_double = s.contains('"');
	if has_single && has_double {
		return Err(RenderError::UnrepresentableText);
	}
	if has_single {
		Ok(format!("\"{}\"", s))
	} else {
		Ok(format!("'{}'", s))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{
		blob::Blob, date::Date, datetime::DateTime, duration::Duration, time::Time, value_type::ValueType,
	};

	use super::*;

	#[test]
	fn none_renders_as_none() {
		assert_eq!(render_value(&Value::none()).unwrap(), "none");
	}

	#[test]
	fn booleans_render_unquoted() {
		assert_eq!(render_value(&Value::Boolean(true)).unwrap(), "true");
		assert_eq!(render_value(&Value::Boolean(false)).unwrap(), "false");
	}

	#[test]
	fn integers_render_as_decimal_including_negatives() {
		assert_eq!(render_value(&Value::Int4(-100)).unwrap(), "-100");
		assert_eq!(render_value(&Value::Int1(i8::MIN)).unwrap(), "-128");
		assert_eq!(render_value(&Value::Uint8(u64::MAX)).unwrap(), "18446744073709551615");
	}

	#[test]
	fn finite_floats_render_and_non_finite_fails_loud() {
		assert_eq!(render_value(&Value::float8(0.0)).unwrap(), "0");
		assert_eq!(render_value(&Value::float8(-2.5)).unwrap(), "-2.5");
		assert_eq!(render_value(&Value::float8(f64::INFINITY)), Err(RenderError::NonFiniteFloat));
		assert_eq!(render_value(&Value::float4(f32::NEG_INFINITY)), Err(RenderError::NonFiniteFloat));
	}

	#[test]
	fn text_quote_selection_and_unrepresentable() {
		assert_eq!(render_value(&Value::Utf8("Alice".to_string())).unwrap(), "'Alice'");
		assert_eq!(render_value(&Value::Utf8(String::new())).unwrap(), "''");
		assert_eq!(render_value(&Value::Utf8("it's".to_string())).unwrap(), "\"it's\"");
		assert_eq!(render_value(&Value::Utf8("say \"hi\"".to_string())).unwrap(), "'say \"hi\"'");
		assert_eq!(
			render_value(&Value::Utf8("both ' and \"".to_string())),
			Err(RenderError::UnrepresentableText)
		);
	}

	#[test]
	fn temporals_render_with_at_prefix() {
		assert_eq!(render_value(&Value::Date(Date::from_ymd(2024, 3, 15).unwrap())).unwrap(), "@2024-03-15");
		assert_eq!(
			render_value(&Value::Time(Time::from_hms_nano(14, 30, 15, 123_456_789).unwrap())).unwrap(),
			"@14:30:15.123456789"
		);
		let dt = render_value(&Value::DateTime(DateTime::from_timestamp(1_700_000_000).unwrap())).unwrap();
		assert!(dt.starts_with("@") && dt.contains('T') && dt.ends_with('Z'), "got {dt}");
	}

	#[test]
	fn durations_render_as_iso_with_at_prefix() {
		assert_eq!(render_value(&Value::Duration(Duration::from_seconds(90).unwrap())).unwrap(), "@PT1M30S");
		assert_eq!(
			render_value(&Value::Duration(Duration::new(14, 3, 3_661_000_000_000).unwrap())).unwrap(),
			"@P1Y2M3DT1H1M1S"
		);
		assert_eq!(render_value(&Value::Duration(Duration::zero()).clone()).unwrap(), "@PT0S");
	}

	#[test]
	fn blob_renders_as_hex_constructor() {
		assert_eq!(
			render_value(&Value::Blob(Blob::from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]))).unwrap(),
			"blob::hex('deadbeef')"
		);
		assert_eq!(render_value(&Value::Blob(Blob::empty())).unwrap(), "blob::hex('')");
	}

	#[test]
	fn dictionary_id_value_is_rejected_since_scans_decode() {
		let _ = ValueType::DictionaryId;
		assert!(matches!(
			render_value(&Value::DictionaryId(Default::default())),
			Err(RenderError::Unsupported(_))
		));
	}
}
