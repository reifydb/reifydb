// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use crate::grpc::client::grpc;
use reifydb_core::FrameColumnData;
use reifydb_core::result::error::diagnostic::{Diagnostic, DiagnosticColumn};
use reifydb_core::result::{Frame, FrameColumn};
use reifydb_core::value::Blob;
use reifydb_core::value::container::{
    BlobContainer, BoolContainer, NumberContainer, RowIdContainer, StringContainer,
    TemporalContainer, UndefinedContainer, UuidContainer,
};
use reifydb_core::value::uuid::{Uuid4, Uuid7};
use reifydb_core::{
    BitVec, Date, DateTime, Interval, OwnedSpan, RowId, SpanColumn, SpanLine, Time, Type,
};
use uuid::Uuid;

pub(crate) fn convert_diagnostic(grpc: grpc::Diagnostic) -> Diagnostic {
    Diagnostic {
        code: grpc.code,
        statement: grpc.statement,
        message: grpc.message,
        span: grpc.span.map(|s| OwnedSpan {
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

    for (_i, grpc_col) in frame.columns.into_iter().enumerate() {
        let data_type = Type::from_u8(grpc_col.ty as u8);
        let frame = grpc_col.frame;
        let name = grpc_col.name;

        let data = match data_type {
            Type::Bool => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::BoolValue(b)) => {
                            data.push(b);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(false);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Bool(BoolContainer::new(data, bitvec))
            }

            Type::Float4 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Float32Value(f)) => {
                            data.push(f);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0.0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Float4(NumberContainer::new(data, bitvec))
            }

            Type::Float8 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Float64Value(f)) => {
                            data.push(f);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0.0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Float8(NumberContainer::new(data, bitvec))
            }

            Type::Int1 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Int1Value(i)) => {
                            data.push(i as i8);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Int1(NumberContainer::new(data, bitvec))
            }

            Type::Int2 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Int2Value(i)) => {
                            data.push(i as i16);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Int2(NumberContainer::new(data, bitvec))
            }

            Type::Int4 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Int4Value(i)) => {
                            data.push(i);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Int4(NumberContainer::new(data, bitvec))
            }

            Type::Int8 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Int8Value(i)) => {
                            data.push(i);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Int8(NumberContainer::new(data, bitvec))
            }

            Type::Int16 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Int16Value(grpc::Int128 { high, low })) => {
                            data.push(((high as i128) << 64) | (low as i128));
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Int16(NumberContainer::new(data, bitvec))
            }

            Type::Uint1 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uint1Value(i)) => {
                            data.push(i as u8);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uint1(NumberContainer::new(data, bitvec))
            }

            Type::Uint2 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uint2Value(i)) => {
                            data.push(i as u16);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uint2(NumberContainer::new(data, bitvec))
            }

            Type::Uint4 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uint4Value(i)) => {
                            data.push(i);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uint4(NumberContainer::new(data, bitvec))
            }

            Type::Uint8 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uint8Value(i)) => {
                            data.push(i);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uint8(NumberContainer::new(data, bitvec))
            }

            Type::Uint16 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uint16Value(grpc::UInt128 { high, low })) => {
                            data.push(((high as u128) << 64) | (low as u128));
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uint16(NumberContainer::new(data, bitvec))
            }

            Type::Utf8 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::StringValue(s)) => {
                            data.push(s);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(String::new());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Utf8(StringContainer::new(data, bitvec))
            }

            Type::Date => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::DateValue(grpc::Date { days_since_epoch })) => {
                            if let Some(date) = Date::from_days_since_epoch(days_since_epoch) {
                                data.push(date);
                                bitvec.push(true);
                            } else {
                                data.push(Date::default());
                                bitvec.push(false);
                            }
                        }
                        _ => {
                            data.push(Date::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Date(TemporalContainer::new(data, bitvec))
            }

            Type::DateTime => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::DatetimeValue(grpc::DateTime { seconds, nanos })) => {
                            if let Ok(datetime) = DateTime::from_parts(seconds, nanos) {
                                data.push(datetime);
                                bitvec.push(true);
                            } else {
                                data.push(DateTime::default());
                                bitvec.push(false);
                            }
                        }
                        _ => {
                            data.push(DateTime::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::DateTime(TemporalContainer::new(data, bitvec))
            }

            Type::Time => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::TimeValue(grpc::Time { nanos_since_midnight })) => {
                            if let Some(time) =
                                Time::from_nanos_since_midnight(nanos_since_midnight)
                            {
                                data.push(time);
                                bitvec.push(true);
                            } else {
                                data.push(Time::default());
                                bitvec.push(false);
                            }
                        }
                        _ => {
                            data.push(Time::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Time(TemporalContainer::new(data, bitvec))
            }

            Type::Interval => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::IntervalValue(grpc::Interval { months, days, nanos })) => {
                            data.push(Interval::new(months, days, nanos));
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Interval::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Interval(TemporalContainer::new(data, bitvec))
            }

            Type::Undefined => {
                FrameColumnData::Undefined(UndefinedContainer::new(grpc_col.data.len()))
            }
            Type::RowId => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::RowIdValue(row_id)) => {
                            data.push(RowId::new(row_id));
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(RowId::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::RowId(RowIdContainer::new(data, bitvec))
            }

            Type::Uuid4 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uuid4Value(bytes)) => {
                            if let Ok(uuid_bytes) = bytes.try_into() {
                                data.push(Uuid4::from(Uuid::from_bytes(uuid_bytes)));
                                bitvec.push(true);
                            } else {
                                data.push(Uuid4::default());
                                bitvec.push(false);
                            }
                        }
                        _ => {
                            data.push(Uuid4::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uuid4(UuidContainer::new(data, bitvec))
            }

            Type::Uuid7 => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::Uuid7Value(bytes)) => {
                            if let Ok(uuid_bytes) = bytes.try_into() {
                                data.push(Uuid7::from(Uuid::from_bytes(uuid_bytes)));
                                bitvec.push(true);
                            } else {
                                data.push(Uuid7::default());
                                bitvec.push(false);
                            }
                        }
                        _ => {
                            data.push(Uuid7::default());
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Uuid7(UuidContainer::new(data, bitvec))
            }

            Type::Blob => {
                let mut data = Vec::with_capacity(grpc_col.data.len());
                let mut bitvec = BitVec::with_capacity(grpc_col.data.len());
                for v in grpc_col.data {
                    match v.r#type {
                        Some(GrpcType::BlobValue(bytes)) => {
                            data.push(Blob::new(bytes));
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Blob::new(vec![]));
                            bitvec.push(false);
                        }
                    }
                }
                FrameColumnData::Blob(BlobContainer::new(data, bitvec))
            }
        };

        // Use the provided metadata, fallback to name if fields are empty
        let name = if name.is_empty() { name.clone() } else { name };

        columns.push(FrameColumn { schema: None, table: frame.clone(), name: name.clone(), data });
        let _qualified_name = if name.contains('.') {
            name.clone()
        } else {
            match &frame {
                Some(sf) => format!("{}.{}", sf, name),
                None => name.clone(),
            }
        };
    }

    Frame::new(columns)
}
