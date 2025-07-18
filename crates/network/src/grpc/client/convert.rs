// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use crate::grpc::client::grpc;
use reifydb_core::diagnostic::{Diagnostic, DiagnosticColumn};
use reifydb_core::{Date, DateTime, Interval, Span, SpanColumn, SpanLine, Time, Type};
use reifydb_engine::frame::{ColumnValues, Frame, FrameColumn};
use std::collections::HashMap;

pub(crate) fn convert_diagnostic(grpc: grpc::Diagnostic) -> Diagnostic {
    Diagnostic {
        code: grpc.code,
        statement: grpc.statement,
        message: grpc.message,
        span: grpc.span.map(|s| Span {
            column: SpanColumn(s.offset),
            line: SpanLine(s.line),
            fragment: s.fragment,
        }),
        label: grpc.label,
        help: grpc.help,
        notes: grpc.notes,
        column: grpc
            .column
            .map(|c| DiagnosticColumn { name: c.name, ty: Type::from_u8(c.ty as u8) }),
        cause: grpc.cause.map(|cb| Box::from(convert_diagnostic(*cb))),
    }
}

pub(crate) fn convert_frame(frame: grpc::Frame) -> Frame {
    use grpc::value::Type as GrpcType;

    let mut columns = Vec::with_capacity(frame.columns.len());
    let mut index = HashMap::with_capacity(frame.columns.len());

    for (i, grpc_col) in frame.columns.into_iter().enumerate() {
        let data_type = Type::from_u8(grpc_col.ty as u8);
        let name = grpc_col.name;

        let values = grpc_col.values;

        let column_values = match data_type {
            Type::Bool => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::BoolValue(b)) => {
                            data.push(b);
                            validity.push(true);
                        }
                        _ => {
                            data.push(false);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::bool_with_bitvec(data, validity)
            }

            Type::Float4 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Float32Value(f)) => {
                            data.push(f);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0.0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::float4_with_bitvec(data, validity)
            }

            Type::Float8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Float64Value(f)) => {
                            data.push(f);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0.0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::float8_with_bitvec(data, validity)
            }

            Type::Int1 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Int1Value(i)) => {
                            data.push(i as i8);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int1_with_bitvec(data, validity)
            }

            Type::Int2 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Int2Value(i)) => {
                            data.push(i as i16);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int2_with_bitvec(data, validity)
            }

            Type::Int4 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Int4Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int4_with_bitvec(data, validity)
            }

            Type::Int8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Int8Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int8_with_bitvec(data, validity)
            }

            Type::Int16 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Int16Value(grpc::Int128 { high, low })) => {
                            data.push(((high as i128) << 64) | (low as i128));
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::int16_with_bitvec(data, validity)
            }

            Type::Uint1 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Uint1Value(i)) => {
                            data.push(i as u8);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint1_with_bitvec(data, validity)
            }

            Type::Uint2 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Uint2Value(i)) => {
                            data.push(i as u16);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint2_with_bitvec(data, validity)
            }

            Type::Uint4 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Uint4Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint4_with_bitvec(data, validity)
            }

            Type::Uint8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Uint8Value(i)) => {
                            data.push(i);
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint8_with_bitvec(data, validity)
            }

            Type::Uint16 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::Uint16Value(grpc::UInt128 { high, low })) => {
                            data.push(((high as u128) << 64) | (low as u128));
                            validity.push(true);
                        }
                        _ => {
                            data.push(0);
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::uint16_with_bitvec(data, validity)
            }

            Type::Utf8 => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::StringValue(s)) => {
                            data.push(s);
                            validity.push(true);
                        }
                        _ => {
                            data.push(String::new());
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::utf8_with_bitvec(data, validity)
            }

            Type::Date => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::DateValue(grpc::Date { days_since_epoch })) => {
                            if let Some(date) = Date::from_days_since_epoch(days_since_epoch) {
                                data.push(date);
                                validity.push(true);
                            } else {
                                data.push(Date::default());
                                validity.push(false);
                            }
                        }
                        _ => {
                            data.push(Date::default());
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::date_with_bitvec(data, validity)
            }

            Type::DateTime => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::DatetimeValue(grpc::DateTime { seconds, nanos })) => {
                            if let Ok(datetime) = DateTime::from_parts(seconds, nanos) {
                                data.push(datetime);
                                validity.push(true);
                            } else {
                                data.push(DateTime::default());
                                validity.push(false);
                            }
                        }
                        _ => {
                            data.push(DateTime::default());
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::datetime_with_bitvec(data, validity)
            }

            Type::Time => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::TimeValue(grpc::Time { nanos_since_midnight })) => {
                            if let Some(time) =
                                Time::from_nanos_since_midnight(nanos_since_midnight)
                            {
                                data.push(time);
                                validity.push(true);
                            } else {
                                data.push(Time::default());
                                validity.push(false);
                            }
                        }
                        _ => {
                            data.push(Time::default());
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::time_with_bitvec(data, validity)
            }

            Type::Interval => {
                let mut data = Vec::with_capacity(values.len());
                let mut validity = Vec::with_capacity(values.len());
                for v in values {
                    match v.r#type {
                        Some(GrpcType::IntervalValue(grpc::Interval { nanos })) => {
                            data.push(Interval::from_nanos(nanos));
                            validity.push(true);
                        }
                        _ => {
                            data.push(Interval::default());
                            validity.push(false);
                        }
                    }
                }
                ColumnValues::interval_with_bitvec(data, validity)
            }

            Type::Undefined => ColumnValues::undefined(values.len()),
        };

        columns.push(FrameColumn { name: name.clone(), values: column_values });
        index.insert(name, i);
    }

    Frame { name: frame.name, columns, index }
}
