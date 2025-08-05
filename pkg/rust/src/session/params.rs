// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{OrderedF32, OrderedF64, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum RqlParams {
    None,
    Positional(Vec<Value>),
    Named(HashMap<String, Value>),
}

impl From<()> for RqlParams {
    fn from(_: ()) -> Self {
        RqlParams::None
    }
}

impl<const N: usize> From<[Value; N]> for RqlParams {
    fn from(values: [Value; N]) -> Self {
        RqlParams::Positional(values.to_vec())
    }
}

impl From<Vec<Value>> for RqlParams {
    fn from(values: Vec<Value>) -> Self {
        RqlParams::Positional(values)
    }
}

impl From<HashMap<String, Value>> for RqlParams {
    fn from(map: HashMap<String, Value>) -> Self {
        RqlParams::Named(map)
    }
}

pub type RqlValue = Value;

impl RqlParams {
    pub fn substitute(&self, rql: &str) -> crate::Result<String> {
        match self {
            RqlParams::None => Ok(rql.to_string()),
            RqlParams::Positional(values) => substitute_positional(rql, values),
            RqlParams::Named(map) => substitute_named(rql, map),
        }
    }
}

fn substitute_positional(rql: &str, values: &[Value]) -> crate::Result<String> {
    let mut result = rql.to_string();
    
    for (idx, value) in values.iter().enumerate() {
        let placeholder = format!("${}", idx + 1);
        let escaped = escape_value(value)?;
        result = result.replace(&placeholder, &escaped);
    }
    
    Ok(result)
}

fn substitute_named(rql: &str, params: &HashMap<String, Value>) -> crate::Result<String> {
    let mut result = rql.to_string();
    
    for (name, value) in params {
        let placeholder = format!("${}", name);
        let escaped = escape_value(value)?;
        result = result.replace(&placeholder, &escaped);
    }
    
    Ok(result)
}

fn escape_value(value: &Value) -> crate::Result<String> {
    Ok(match value {
        Value::Undefined => "undefined".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Float4(OrderedF32(f)) => f.to_string(),
        Value::Float8(OrderedF64(f)) => f.to_string(),
        Value::Int1(n) => n.to_string(),
        Value::Int2(n) => n.to_string(),
        Value::Int4(n) => n.to_string(),
        Value::Int8(n) => n.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Uint1(n) => n.to_string(),
        Value::Uint2(n) => n.to_string(),
        Value::Uint4(n) => n.to_string(),
        Value::Uint8(n) => n.to_string(),
        Value::Uint16(n) => n.to_string(),
        Value::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Date(d) => format!("date('{}')", d),
        Value::DateTime(dt) => format!("datetime('{}')", dt),
        Value::Time(t) => format!("time('{}')", t),
        Value::Interval(i) => format!("interval('{}')", i),
        Value::Uuid4(u) => format!("uuid4('{}')", u),
        Value::Uuid7(u) => format!("uuid7('{}')", u),
        Value::Blob(b) => {
            use base64::{Engine as _, engine::general_purpose};
            format!("blob::base64('{}')", general_purpose::STANDARD.encode(b.as_ref()))
        },
        Value::RowId(id) => format!("rowid({})", id),
    })
}

#[macro_export]
macro_rules! params {
    () => {
        $crate::session::RqlParams::None
    };
    
    // Named parameters: params! { "name" => value, ... }
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut map = std::collections::HashMap::new();
        $(
            map.insert($key.to_string(), $value.into());
        )*
        $crate::session::RqlParams::Named(map)
    }};
    
    // Positional parameters: params![value1, value2, ...]
    [$($value:expr),* $(,)?] => {
        $crate::session::RqlParams::Positional(vec![$($value.into()),*])
    };
}

pub use params;