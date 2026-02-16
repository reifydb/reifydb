// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, util::bitvec::BitVec, value::Value};

use crate::value::column::data::ColumnData;

macro_rules! push_or_promote {
	// Helper: wrap a column in Option if there are preceding undefined values
	(@wrap_option $self:expr, $new_col:expr, $len:expr) => {
		if $len > 0 {
			let mut bitvec = BitVec::repeat($len, false);
			DataBitVec::push(&mut bitvec, true);
			*$self = ColumnData::Option {
				inner: Box::new($new_col),
				bitvec,
			};
		} else {
			*$self = $new_col;
		}
	};
	// Tuple variant, uses self.push(value)
	($self:expr, $val:expr, $col_variant:ident, $factory_expr:expr) => {
		match $self {
			ColumnData::$col_variant(_) => $self.push($val),
			ColumnData::Undefined(container) => {
				let len = container.len();
				let mut new_col = $factory_expr;
				if let ColumnData::$col_variant(nc) = &mut new_col {
					for _ in 0..len {
						nc.push_undefined();
					}
					nc.push($val);
				}
				push_or_promote!(@wrap_option $self, new_col, len);
			}
			_ => unimplemented!(),
		}
	};
	// Struct variant, uses self.push(value)
	(struct $self:expr, $val:expr, $col_variant:ident, $factory_expr:expr) => {
		match $self {
			ColumnData::$col_variant {
				..
			} => $self.push($val),
			ColumnData::Undefined(container) => {
				let len = container.len();
				let mut new_col = $factory_expr;
				if let ColumnData::$col_variant {
					container: nc,
					..
				} = &mut new_col
				{
					for _ in 0..len {
						nc.push_undefined();
					}
					nc.push($val);
				}
				push_or_promote!(@wrap_option $self, new_col, len);
			}
			_ => unimplemented!(),
		}
	};
	// Tuple variant, direct container push (no Push trait impl needed)
	(direct $self:expr, $val:expr, $col_variant:ident, $factory_expr:expr) => {
		match $self {
			ColumnData::$col_variant(container) => container.push($val),
			ColumnData::Undefined(container) => {
				let len = container.len();
				let mut new_col = $factory_expr;
				if let ColumnData::$col_variant(nc) = &mut new_col {
					for _ in 0..len {
						nc.push_undefined();
					}
					nc.push($val);
				}
				push_or_promote!(@wrap_option $self, new_col, len);
			}
			_ => unimplemented!(),
		}
	};
	// Struct variant, direct container push
	(struct_direct $self:expr, $val:expr, $col_variant:ident, $factory_expr:expr) => {
		match $self {
			ColumnData::$col_variant {
				container,
				..
			} => container.push($val),
			ColumnData::Undefined(container) => {
				let len = container.len();
				let mut new_col = $factory_expr;
				if let ColumnData::$col_variant {
					container: nc,
					..
				} = &mut new_col
				{
					for _ in 0..len {
						nc.push_undefined();
					}
					nc.push($val);
				}
				push_or_promote!(@wrap_option $self, new_col, len);
			}
			_ => unimplemented!(),
		}
	};
}

impl ColumnData {
	pub fn push_value(&mut self, value: Value) {
		// Handle Option wrapper: delegate to inner and update bitvec
		if let ColumnData::Option {
			inner,
			bitvec,
		} = self
		{
			if matches!(value, Value::None) {
				inner.push_undefined();
				DataBitVec::push(bitvec, false);
			} else {
				inner.push_value(value);
				DataBitVec::push(bitvec, true);
			}
			return;
		}
		match value {
			Value::Boolean(v) => push_or_promote!(self, v, Bool, ColumnData::bool(vec![])),
			Value::Float4(v) => push_or_promote!(self, v.value(), Float4, ColumnData::float4(vec![])),
			Value::Float8(v) => push_or_promote!(self, v.value(), Float8, ColumnData::float8(vec![])),
			Value::Int1(v) => push_or_promote!(self, v, Int1, ColumnData::int1(vec![])),
			Value::Int2(v) => push_or_promote!(self, v, Int2, ColumnData::int2(vec![])),
			Value::Int4(v) => push_or_promote!(self, v, Int4, ColumnData::int4(vec![])),
			Value::Int8(v) => push_or_promote!(self, v, Int8, ColumnData::int8(vec![])),
			Value::Int16(v) => push_or_promote!(self, v, Int16, ColumnData::int16(vec![])),
			Value::Uint1(v) => push_or_promote!(self, v, Uint1, ColumnData::uint1(vec![])),
			Value::Uint2(v) => push_or_promote!(self, v, Uint2, ColumnData::uint2(vec![])),
			Value::Uint4(v) => push_or_promote!(self, v, Uint4, ColumnData::uint4(vec![])),
			Value::Uint8(v) => push_or_promote!(self, v, Uint8, ColumnData::uint8(vec![])),
			Value::Uint16(v) => push_or_promote!(self, v, Uint16, ColumnData::uint16(vec![])),
			Value::Utf8(v) => {
				push_or_promote!(struct self, v, Utf8, ColumnData::utf8(Vec::<String>::new()))
			}
			Value::Date(v) => push_or_promote!(self, v, Date, ColumnData::date(vec![])),
			Value::DateTime(v) => push_or_promote!(self, v, DateTime, ColumnData::datetime(vec![])),
			Value::Time(v) => push_or_promote!(self, v, Time, ColumnData::time(vec![])),
			Value::Duration(v) => push_or_promote!(self, v, Duration, ColumnData::duration(vec![])),
			Value::Uuid4(v) => push_or_promote!(self, v, Uuid4, ColumnData::uuid4(vec![])),
			Value::Uuid7(v) => push_or_promote!(self, v, Uuid7, ColumnData::uuid7(vec![])),
			Value::IdentityId(v) => {
				push_or_promote!(direct self, v, IdentityId, ColumnData::identity_id(vec![]))
			}
			Value::DictionaryId(v) => {
				push_or_promote!(direct self, v, DictionaryId, ColumnData::dictionary_id(vec![]))
			}
			Value::Blob(v) => push_or_promote!(struct_direct self, v, Blob, ColumnData::blob(vec![])),
			Value::Int(v) => push_or_promote!(struct_direct self, v, Int, ColumnData::int(vec![])),
			Value::Uint(v) => push_or_promote!(struct_direct self, v, Uint, ColumnData::uint(vec![])),
			Value::Decimal(v) => {
				push_or_promote!(struct_direct self, v, Decimal, ColumnData::decimal(vec![]))
			}
			Value::None => self.push_undefined(),
			Value::Type(t) => self.push_value(Value::Any(Box::new(Value::Type(t)))),
			Value::Any(v) => match self {
				ColumnData::Any(container) => container.push(v),
				ColumnData::Undefined(container) => {
					let len = container.len();
					let mut new_col = ColumnData::any(vec![]);
					if let ColumnData::Any(nc) = &mut new_col {
						for _ in 0..len {
							nc.push_undefined();
						}
						nc.push(v);
					}
					push_or_promote!(@wrap_option self, new_col, len);
				}
				_ => unreachable!("Cannot push Any value to non-Any column"),
			},
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use reifydb_type::value::{
		Value,
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
	};

	use crate::value::column::ColumnData;

	#[test]
	fn test_bool() {
		let mut col = ColumnData::bool(vec![true]);
		col.push_value(Value::Boolean(false));
		let ColumnData::Bool(container) = col else {
			panic!("Expected Bool");
		};
		assert_eq!(container.data().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_undefined_bool() {
		let mut col = ColumnData::bool(vec![true]);
		col.push_value(Value::None);
		// push_value(None) promotes to Option-wrapped; check via ColumnData API
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_bool() {
		let mut col = ColumnData::undefined(2);
		col.push_value(Value::Boolean(true));
		assert_eq!(col.len(), 3);
		assert!(!col.is_defined(0));
		assert!(!col.is_defined(1));
		assert!(col.is_defined(2));
		assert_eq!(col.get_value(2), Value::Boolean(true));
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnData::float4(vec![1.0]);
		col.push_value(Value::Float4(OrderedF32::try_from(2.0).unwrap()));
		let ColumnData::Float4(container) = col else {
			panic!("Expected Float4");
		};
		assert_eq!(container.data().as_slice(), &[1.0, 2.0]);
	}

	#[test]
	fn test_undefined_float4() {
		let mut col = ColumnData::float4(vec![1.0]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_float4() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Float4(OrderedF32::try_from(3.14).unwrap()));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnData::float8(vec![1.0]);
		col.push_value(Value::Float8(OrderedF64::try_from(2.0).unwrap()));
		let ColumnData::Float8(container) = col else {
			panic!("Expected Float8");
		};
		assert_eq!(container.data().as_slice(), &[1.0, 2.0]);
	}

	#[test]
	fn test_undefined_float8() {
		let mut col = ColumnData::float8(vec![1.0]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_float8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Float8(OrderedF64::try_from(2.718).unwrap()));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnData::int1(vec![1]);
		col.push_value(Value::Int1(2));
		let ColumnData::Int1(container) = col else {
			panic!("Expected Int1");
		};
		assert_eq!(container.data().as_slice(), &[1, 2]);
	}

	#[test]
	fn test_undefined_int1() {
		let mut col = ColumnData::int1(vec![1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_int1() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int1(5));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int1(5));
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_value(Value::Int2(3));
		let ColumnData::Int2(container) = col else {
			panic!("Expected Int2");
		};
		assert_eq!(container.data().as_slice(), &[1, 3]);
	}

	#[test]
	fn test_undefined_int2() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_int2() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int2(10));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int2(10));
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnData::int4(vec![10]);
		col.push_value(Value::Int4(20));
		let ColumnData::Int4(container) = col else {
			panic!("Expected Int4");
		};
		assert_eq!(container.data().as_slice(), &[10, 20]);
	}

	#[test]
	fn test_undefined_int4() {
		let mut col = ColumnData::int4(vec![10]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_int4() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int4(20));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int4(20));
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnData::int8(vec![100]);
		col.push_value(Value::Int8(200));
		let ColumnData::Int8(container) = col else {
			panic!("Expected Int8");
		};
		assert_eq!(container.data().as_slice(), &[100, 200]);
	}

	#[test]
	fn test_undefined_int8() {
		let mut col = ColumnData::int8(vec![100]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_int8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int8(30));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int8(30));
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnData::int16(vec![1000]);
		col.push_value(Value::Int16(2000));
		let ColumnData::Int16(container) = col else {
			panic!("Expected Int16");
		};
		assert_eq!(container.data().as_slice(), &[1000, 2000]);
	}

	#[test]
	fn test_undefined_int16() {
		let mut col = ColumnData::int16(vec![1000]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_int16() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int16(40));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int16(40));
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnData::uint1(vec![1]);
		col.push_value(Value::Uint1(2));
		let ColumnData::Uint1(container) = col else {
			panic!("Expected Uint1");
		};
		assert_eq!(container.data().as_slice(), &[1, 2]);
	}

	#[test]
	fn test_undefined_uint1() {
		let mut col = ColumnData::uint1(vec![1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uint1() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint1(1));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint1(1));
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnData::uint2(vec![10]);
		col.push_value(Value::Uint2(20));
		let ColumnData::Uint2(container) = col else {
			panic!("Expected Uint2");
		};
		assert_eq!(container.data().as_slice(), &[10, 20]);
	}

	#[test]
	fn test_undefined_uint2() {
		let mut col = ColumnData::uint2(vec![10]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uint2() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint2(2));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint2(2));
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnData::uint4(vec![100]);
		col.push_value(Value::Uint4(200));
		let ColumnData::Uint4(container) = col else {
			panic!("Expected Uint4");
		};
		assert_eq!(container.data().as_slice(), &[100, 200]);
	}

	#[test]
	fn test_undefined_uint4() {
		let mut col = ColumnData::uint4(vec![100]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uint4() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint4(3));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint4(3));
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnData::uint8(vec![1000]);
		col.push_value(Value::Uint8(2000));
		let ColumnData::Uint8(container) = col else {
			panic!("Expected Uint8");
		};
		assert_eq!(container.data().as_slice(), &[1000, 2000]);
	}

	#[test]
	fn test_undefined_uint8() {
		let mut col = ColumnData::uint8(vec![1000]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uint8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint8(4));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint8(4));
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnData::uint16(vec![10000]);
		col.push_value(Value::Uint16(20000));
		let ColumnData::Uint16(container) = col else {
			panic!("Expected Uint16");
		};
		assert_eq!(container.data().as_slice(), &[10000, 20000]);
	}

	#[test]
	fn test_undefined_uint16() {
		let mut col = ColumnData::uint16(vec![10000]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uint16() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint16(5));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint16(5));
	}

	#[test]
	fn test_utf8() {
		let mut col = ColumnData::utf8(vec!["hello".to_string()]);
		col.push_value(Value::Utf8("world".to_string()));
		let ColumnData::Utf8 {
			container,
			..
		} = col
		else {
			panic!("Expected Utf8");
		};
		assert_eq!(container.data().as_slice(), &["hello".to_string(), "world".to_string()]);
	}

	#[test]
	fn test_undefined_utf8() {
		let mut col = ColumnData::utf8(vec!["hello".to_string()]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_utf8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Utf8("ok".to_string()));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Utf8("ok".to_string()));
	}

	#[test]
	fn test_undefined() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let date2 = Date::from_ymd(2023, 12, 31).unwrap();
		let mut col = ColumnData::date(vec![date1]);
		col.push_value(Value::Date(date2));
		let ColumnData::Date(container) = col else {
			panic!("Expected Date");
		};
		assert_eq!(container.data().as_slice(), &[date1, date2]);
	}

	#[test]
	fn test_undefined_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let mut col = ColumnData::date(vec![date1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_date() {
		let date = Date::from_ymd(2023, 6, 15).unwrap();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Date(date));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Date(date));
	}

	#[test]
	fn test_datetime() {
		let dt1 = DateTime::from_timestamp(1672531200).unwrap(); // 2023-01-01 00:00:00 SVTC
		let dt2 = DateTime::from_timestamp(1704067200).unwrap(); // 2024-01-01 00:00:00 SVTC
		let mut col = ColumnData::datetime(vec![dt1]);
		col.push_value(Value::DateTime(dt2));
		let ColumnData::DateTime(container) = col else {
			panic!("Expected DateTime");
		};
		assert_eq!(container.data().as_slice(), &[dt1, dt2]);
	}

	#[test]
	fn test_undefined_datetime() {
		let dt1 = DateTime::from_timestamp(1672531200).unwrap();
		let mut col = ColumnData::datetime(vec![dt1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_datetime() {
		let dt = DateTime::from_timestamp(1672531200).unwrap();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::DateTime(dt));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::DateTime(dt));
	}

	#[test]
	fn test_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let time2 = Time::from_hms(18, 45, 30).unwrap();
		let mut col = ColumnData::time(vec![time1]);
		col.push_value(Value::Time(time2));
		let ColumnData::Time(container) = col else {
			panic!("Expected Time");
		};
		assert_eq!(container.data().as_slice(), &[time1, time2]);
	}

	#[test]
	fn test_undefined_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let mut col = ColumnData::time(vec![time1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_time() {
		let time = Time::from_hms(15, 20, 10).unwrap();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Time(time));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Time(time));
	}

	#[test]
	fn test_duration() {
		let duration1 = Duration::from_days(30);
		let duration2 = Duration::from_hours(24);
		let mut col = ColumnData::duration(vec![duration1]);
		col.push_value(Value::Duration(duration2));
		let ColumnData::Duration(container) = col else {
			panic!("Expected Duration");
		};
		assert_eq!(container.data().as_slice(), &[duration1, duration2]);
	}

	#[test]
	fn test_undefined_duration() {
		let duration1 = Duration::from_days(30);
		let mut col = ColumnData::duration(vec![duration1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_duration() {
		let duration = Duration::from_minutes(90);
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Duration(duration));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Duration(duration));
	}

	#[test]
	fn test_identity_id() {
		let id1 = IdentityId::generate();
		let id2 = IdentityId::generate();
		let mut col = ColumnData::identity_id(vec![id1]);
		col.push_value(Value::IdentityId(id2));
		let ColumnData::IdentityId(container) = col else {
			panic!("Expected IdentityId");
		};
		assert_eq!(container.data().as_slice(), &[id1, id2]);
	}

	#[test]
	fn test_undefined_identity_id() {
		let id1 = IdentityId::generate();
		let mut col = ColumnData::identity_id(vec![id1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_identity_id() {
		let id = IdentityId::generate();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::IdentityId(id));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::IdentityId(id));
	}

	#[test]
	fn test_uuid4() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let mut col = ColumnData::uuid4(vec![uuid1]);
		col.push_value(Value::Uuid4(uuid2));
		let ColumnData::Uuid4(container) = col else {
			panic!("Expected Uuid4");
		};
		assert_eq!(container.data().as_slice(), &[uuid1, uuid2]);
	}

	#[test]
	fn test_undefined_uuid4() {
		let uuid1 = Uuid4::generate();
		let mut col = ColumnData::uuid4(vec![uuid1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uuid4() {
		let uuid = Uuid4::generate();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uuid4(uuid));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uuid4(uuid));
	}

	#[test]
	fn test_uuid7() {
		let uuid1 = Uuid7::generate();
		let uuid2 = Uuid7::generate();
		let mut col = ColumnData::uuid7(vec![uuid1]);
		col.push_value(Value::Uuid7(uuid2));
		let ColumnData::Uuid7(container) = col else {
			panic!("Expected Uuid7");
		};
		assert_eq!(container.data().as_slice(), &[uuid1, uuid2]);
	}

	#[test]
	fn test_undefined_uuid7() {
		let uuid1 = Uuid7::generate();
		let mut col = ColumnData::uuid7(vec![uuid1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_uuid7() {
		let uuid = Uuid7::generate();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uuid7(uuid));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uuid7(uuid));
	}

	#[test]
	fn test_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);
		let mut col = ColumnData::dictionary_id(vec![e1]);
		col.push_value(Value::DictionaryId(e2));
		let ColumnData::DictionaryId(container) = col else {
			panic!("Expected DictionaryId");
		};
		assert_eq!(container.data().as_slice(), &[e1, e2]);
	}

	#[test]
	fn test_undefined_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let mut col = ColumnData::dictionary_id(vec![e1]);
		col.push_value(Value::None);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_undefined_dictionary_id() {
		let e = DictionaryEntryId::U4(42);
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::DictionaryId(e));
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::DictionaryId(e));
	}
}
