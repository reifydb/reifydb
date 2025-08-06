// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{self, Visitor};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    #[serde(flatten)]
    pub payload: RequestPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
    Auth(AuthRequest),
    Command(CommandRequest),
    Query(QueryRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
    pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
    pub statements: Vec<String>,
    pub params: Option<WsParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub statements: Vec<String>,
    pub params: Option<WsParams>,
}

#[derive(Debug)]
pub enum WsParams {
    Positional(Vec<Value>),
    Named(HashMap<String, Value>),
}

impl Serialize for WsParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            WsParams::Positional(values) => values.serialize(serializer),
            WsParams::Named(map) => map.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for WsParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WsParamsVisitor;
        
        impl<'de> Visitor<'de> for WsParamsVisitor {
            type Value = WsParams;
            
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an array for positional parameters or an object for named parameters")
            }
            
            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let values = Vec::<Value>::deserialize(de::value::SeqAccessDeserializer::new(seq))?;
                Ok(WsParams::Positional(values))
            }
            
            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let map = HashMap::<String, Value>::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(WsParams::Named(map))
            }
        }
        
        deserializer.deserialize_any(WsParamsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::{Date, DateTime, Interval, Time, Uuid4, Uuid7, Blob, RowId, OrderedF32, OrderedF64};
    
    fn test_roundtrip(params: WsParams) {
        let serialized = serde_json::to_string(&params).unwrap();
        let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        
        match &params {
            WsParams::Positional(_) => assert!(value.is_array()),
            WsParams::Named(_) => assert!(value.is_object()),
        }
        
        let deserialized: WsParams = serde_json::from_str(&serialized).unwrap();
        match (&params, &deserialized) {
            (WsParams::Positional(v1), WsParams::Positional(v2)) => assert_eq!(v1, v2),
            (WsParams::Named(m1), WsParams::Named(m2)) => assert_eq!(m1, m2),
            _ => panic!("Type mismatch after deserialization"),
        }
    }
    
    #[test]
    fn test_undefined() {
        let params = WsParams::Positional(vec![Value::Undefined]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_bool() {
        let params = WsParams::Positional(vec![Value::Bool(true), Value::Bool(false)]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_float4() {
        let params = WsParams::Positional(vec![
            Value::Float4(OrderedF32::try_from(3.14_f32).unwrap()),
            Value::Float4(OrderedF32::try_from(-0.5_f32).unwrap()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_float8() {
        let params = WsParams::Positional(vec![
            Value::Float8(OrderedF64::try_from(3.14159265_f64).unwrap()),
            Value::Float8(OrderedF64::try_from(-0.00001_f64).unwrap()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_int1() {
        let params = WsParams::Positional(vec![
            Value::Int1(i8::MIN),
            Value::Int1(i8::MAX),
            Value::Int1(0),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_int2() {
        let params = WsParams::Positional(vec![
            Value::Int2(i16::MIN),
            Value::Int2(i16::MAX),
            Value::Int2(0),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_int4() {
        let params = WsParams::Positional(vec![
            Value::Int4(i32::MIN),
            Value::Int4(i32::MAX),
            Value::Int4(0),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_int8() {
        let params = WsParams::Positional(vec![
            Value::Int8(i64::MIN),
            Value::Int8(i64::MAX),
            Value::Int8(0),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_int16() {
        let params = WsParams::Positional(vec![
            Value::Int16(i128::MIN),
            Value::Int16(i128::MAX),
            Value::Int16(0),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_utf8() {
        let params = WsParams::Positional(vec![
            Value::Utf8("Hello".to_string()),
            Value::Utf8("世界".to_string()),
            Value::Utf8("".to_string()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uint1() {
        let params = WsParams::Positional(vec![
            Value::Uint1(u8::MIN),
            Value::Uint1(u8::MAX),
            Value::Uint1(128),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uint2() {
        let params = WsParams::Positional(vec![
            Value::Uint2(u16::MIN),
            Value::Uint2(u16::MAX),
            Value::Uint2(32768),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uint4() {
        let params = WsParams::Positional(vec![
            Value::Uint4(u32::MIN),
            Value::Uint4(u32::MAX),
            Value::Uint4(2147483648),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uint8() {
        let params = WsParams::Positional(vec![
            Value::Uint8(u64::MIN),
            Value::Uint8(u64::MAX),
            Value::Uint8(9223372036854775808),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uint16() {
        let params = WsParams::Positional(vec![
            Value::Uint16(u128::MIN),
            Value::Uint16(u128::MAX),
            Value::Uint16(170141183460469231731687303715884105728),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_date() {
        let params = WsParams::Positional(vec![
            Value::Date(Date::new(2025, 1, 15).unwrap()),
            Value::Date(Date::new(1970, 1, 1).unwrap()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_datetime() {
        let params = WsParams::Positional(vec![
            Value::DateTime(DateTime::now()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_time() {
        let params = WsParams::Positional(vec![
            Value::Time(Time::new(12, 30, 45, 0).unwrap()),
            Value::Time(Time::new(0, 0, 0, 0).unwrap()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_interval() {
        let params = WsParams::Positional(vec![
            Value::Interval(Interval::from_seconds(3600)),
            Value::Interval(Interval::from_seconds(0)),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_row_id() {
        let params = WsParams::Positional(vec![
            Value::RowId(RowId::from(12345u64)),
            Value::RowId(RowId::from(0u64)),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uuid4() {
        let params = WsParams::Positional(vec![
            Value::Uuid4(Uuid4::generate()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_uuid7() {
        let params = WsParams::Positional(vec![
            Value::Uuid7(Uuid7::generate()),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_blob() {
        let params = WsParams::Positional(vec![
            Value::Blob(Blob::from(vec![1, 2, 3, 4, 5])),
            Value::Blob(Blob::from(vec![])),
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_named_params() {
        let mut map = HashMap::new();
        map.insert("id".to_string(), Value::Int4(42));
        map.insert("name".to_string(), Value::Utf8("Alice".to_string()));
        map.insert("active".to_string(), Value::Bool(true));
        
        let params = WsParams::Named(map);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_mixed_types_positional() {
        let params = WsParams::Positional(vec![
            Value::Bool(true),
            Value::Int4(42),
            Value::Utf8("test".to_string()),
            Value::Float8(OrderedF64::try_from(3.14).unwrap()),
            Value::Undefined,
        ]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_mixed_types_named() {
        let mut map = HashMap::new();
        map.insert("bool_val".to_string(), Value::Bool(false));
        map.insert("int_val".to_string(), Value::Int8(9999));
        map.insert("float_val".to_string(), Value::Float4(OrderedF32::try_from(2.5).unwrap()));
        map.insert("text_val".to_string(), Value::Utf8("hello world".to_string()));
        map.insert("date_val".to_string(), Value::Date(Date::new(2025, 1, 15).unwrap()));
        
        let params = WsParams::Named(map);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_empty_positional() {
        let params = WsParams::Positional(vec![]);
        test_roundtrip(params);
    }
    
    #[test]
    fn test_empty_named() {
        let params = WsParams::Named(HashMap::new());
        test_roundtrip(params);
    }
    
    #[test]
    fn test_json_format_array() {
        let json = r#"[{"Int4":42},{"Utf8":"hello"},{"Bool":true}]"#;
        let params: WsParams = serde_json::from_str(json).unwrap();
        
        match &params {
            WsParams::Positional(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::Int4(42));
                assert_eq!(values[1], Value::Utf8("hello".to_string()));
                assert_eq!(values[2], Value::Bool(true));
            }
            _ => panic!("Expected positional parameters"),
        }
        
        // Verify it serializes back to the same format
        let serialized = serde_json::to_string(&params).unwrap();
        assert_eq!(serialized, json);
    }
    
    #[test]
    fn test_json_format_object() {
        let json = r#"{"id":{"Int4":42},"name":{"Utf8":"Alice"},"active":{"Bool":true}}"#;
        let params: WsParams = serde_json::from_str(json).unwrap();
        
        match &params {
            WsParams::Named(map) => {
                assert_eq!(map.len(), 3);
                assert_eq!(map.get("id"), Some(&Value::Int4(42)));
                assert_eq!(map.get("name"), Some(&Value::Utf8("Alice".to_string())));
                assert_eq!(map.get("active"), Some(&Value::Bool(true)));
            }
            _ => panic!("Expected named parameters"),
        }
    }
}
