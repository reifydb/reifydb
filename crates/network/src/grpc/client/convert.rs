// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use crate::grpc::client::grpc;
use reifydb_core::{Diagnostic, DiagnosticColumn, Kind, Line, Offset, Span};
use reifydb_engine::frame::{Column, ColumnValues, Frame};
use std::collections::HashMap;

pub(crate) fn convert_diagnostic(grpc: grpc::Diagnostic) -> Diagnostic {
    Diagnostic {
        code: grpc.code,
        statement: grpc.statement,
        message: grpc.message,
        span: grpc.span.map(|s| Span {
            offset: Offset(s.offset),
            line: Line(s.line),
            fragment: s.fragment,
        }),
        label: grpc.label,
        help: grpc.help,
        notes: grpc.notes,
        column: grpc
            .column
            .map(|c| DiagnosticColumn { name: c.name, value: Kind::from_u8(c.kind as u8) }),
    }
}

pub(crate) fn convert_frame(frame: grpc::Frame) -> Frame {
    use grpc::value::Kind as GrpcValueKind;

    let mut columns = Vec::with_capacity(frame.columns.len());
    let mut index = HashMap::with_capacity(frame.columns.len());

    for (i, grpc_col) in frame.columns.into_iter().enumerate() {
        let kind = Kind::from_u8(grpc_col.kind as u8);
        let name = grpc_col.name;

        let values = grpc_col.values;

        let column_values = match kind {
            Kind::Bool => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::BoolValue(b)) => {
                            data.push(b);
                            validity.push(true);
                        }
                        _ => {
                            data.push(false);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::bool_with_validity(data, validity)
            }

            Kind::Float4 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Float32Value(f)) => {
                            data.push(f);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0.0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::float4_with_validity(data, validity)
            }

            Kind::Float8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Float64Value(f)) => {
                            data.push(f);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0.0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::float8_with_validity(data, validity)
            }

            Kind::Int1 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Int1Value(i)) => {
                            data.push(i as i8);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int1_with_validity(data, validity)
            }

            Kind::Int2 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Int2Value(i)) => {
                            data.push(i as i16);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int2_with_validity(data, validity)
            }

            Kind::Int4 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Int4Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int4_with_validity(data, validity)
            }

            Kind::Int8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Int8Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int8_with_validity(data, validity)
            }

            Kind::Int16 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Int16Value(grpc::Int128 { high, low })) => {
                            data.push(((high as i128) << 64) | (low as i128));
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int16_with_validity(data, validity)
            }

            Kind::Uint1 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Uint1Value(i)) => {
                            data.push(i as u8);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint1_with_validity(data, validity)
            }

            Kind::Uint2 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Uint2Value(i)) => {
                            data.push(i as u16);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint2_with_validity(data, validity)
            }

            Kind::Uint4 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Uint4Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint4_with_validity(data, validity)
            }

            Kind::Uint8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Uint8Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint8_with_validity(data, validity)
            }

            Kind::Uint16 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::Uint16Value(grpc::UInt128 { high, low })) => {
                            data.push(((high as u128) << 64) | (low as u128));
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint16_with_validity(data, validity)
            }

            Kind::Utf8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.kind {
                        Some(GrpcValueKind::StringValue(s)) => {
                            data.push(s);
                            validity.push(true);
                        }
                        _ => {
                            data.push(String::new());
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::utf8_with_validity(data, validity)
            }

            Kind::Undefined => ColumnValues::undefined(values.len()),
        };

        columns.push(Column { name: name.clone(), values: column_values });
        index.insert(name, i);
    }

    Frame { name: frame.name, columns, index }
}
