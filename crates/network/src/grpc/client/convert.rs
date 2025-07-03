// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::grpc::client::grpc_db;
use reifydb_core::num::ordered_float::{OrderedF32, OrderedF64};
use reifydb_core::{Diagnostic, DiagnosticColumn, Kind, Line, Offset, Span, Value};

pub(crate) fn convert_diagnostic(grpc: grpc_db::Diagnostic) -> Diagnostic {
    Diagnostic {
        code: grpc.code,
        message: grpc.message,
        span: grpc.span.map(|s| Span {
            offset: Offset(s.offset),
            line: Line(s.line),
            fragment: s.fragment,
        }),
        label: if grpc.label.is_empty() { None } else { Some(grpc.label) },
        help: if grpc.help.is_empty() { None } else { Some(grpc.help) },
        notes: grpc.notes,
        column: grpc
            .column
            .map(|c| DiagnosticColumn { name: c.name, value: Kind::from_u8(c.value as u8) }),
    }
}

pub(crate) fn convert_value(value: grpc_db::Value) -> Value {
    match value.kind.unwrap_or_else(|| panic!("Missing value kind")) {
        grpc_db::value::Kind::BoolValue(b) => Value::Bool(b),
        grpc_db::value::Kind::Float32Value(f) => {
            OrderedF32::try_from(f).ok().map(Value::Float4).unwrap_or(Value::Undefined)
        }
        grpc_db::value::Kind::Float64Value(f) => {
            OrderedF64::try_from(f).ok().map(Value::Float8).unwrap_or(Value::Undefined)
        }
        grpc_db::value::Kind::Int1Value(i) => Value::Int1(i as i8),
        grpc_db::value::Kind::Int2Value(i) => Value::Int2(i as i16),
        grpc_db::value::Kind::Int4Value(i) => Value::Int4(i),
        grpc_db::value::Kind::Int8Value(i) => Value::Int8(i),
        grpc_db::value::Kind::Int16Value(i) => {
            Value::Int16(((i.high as i128) << 64) | i.low as i128)
        }

        grpc_db::value::Kind::Uint1Value(u) => Value::Uint1(u as u8),
        grpc_db::value::Kind::Uint2Value(u) => Value::Uint2(u as u16),
        grpc_db::value::Kind::Uint4Value(u) => Value::Uint4(u),
        grpc_db::value::Kind::Uint8Value(u) => Value::Uint8(u),
        grpc_db::value::Kind::Uint16Value(u) => {
            Value::Uint16(((u.high as u128) << 64) | u.low as u128)
        }

        grpc_db::value::Kind::StringValue(s) => Value::String(s),
        grpc_db::value::Kind::UndefinedValue(_) => Value::Undefined,
    }
}
