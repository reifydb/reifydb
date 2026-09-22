// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBufferBuilder, bit_chunk_iterator::UnalignedBitChunk};
use reifydb_value::value::{
	Value,
	blob::Blob,
	container::{
		decimal_array::uint16_to_native,
		dictionary_array::push_entry,
		temporal_array::{date_to_native, datetime_to_native, duration_to_native, time_to_native},
	},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

use crate::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};

macro_rules! push_or_promote {
	(native $self:expr, $val:expr, $col_variant:ident) => {
		match $self {
			ColumnBuilder::$col_variant(builder) => builder.append_value($val),
			_ => unimplemented!(),
		}
	};

	(temporal $self:expr, $val:expr, $col_variant:ident, $to_native:ident) => {
		match $self {
			ColumnBuilder::$col_variant(builder) => builder.append_value($to_native($val)),
			_ => unimplemented!(),
		}
	};

	(fixed $self:expr, $val:expr, $col_variant:ident) => {
		match $self {
			ColumnBuilder::$col_variant(buffer) => buffer.extend_from_slice($val.as_bytes()),
			_ => unimplemented!(),
		}
	};

	(varlen $self:expr, $val:expr, $col_variant:ident) => {
		match $self {
			ColumnBuilder::$col_variant {
				builder,
				..
			} => builder.append_value($val),
			_ => unimplemented!(),
		}
	};

	(struct_direct $self:expr, $val:expr, $col_variant:ident) => {
		match $self {
			ColumnBuilder::Buffer(ColumnBuffer::$col_variant {
				container,
				..
			}) => container.push($val),
			_ => unimplemented!(),
		}
	};
}

impl ColumnBuilder {
	pub fn push_value(&mut self, value: Value) {
		if let ColumnBuilder::Option {
			inner,
			bitvec,
		} = self
		{
			if matches!(value, Value::None { .. }) {
				inner.push_default();
				bitvec.append(false);
			} else if UnalignedBitChunk::new(bitvec.as_slice(), 0, bitvec.len()).count_ones() == 0 {
				let len = inner.len();

				let mut new_inner = match &value {
					Value::Boolean(_) => ColumnBuffer::bool(vec![false; len]),
					Value::Float4(_) => ColumnBuffer::float4(vec![0.0f32; len]),
					Value::Float8(_) => ColumnBuffer::float8(vec![0.0f64; len]),
					Value::Int1(_) => ColumnBuffer::int1(vec![0i8; len]),
					Value::Int2(_) => ColumnBuffer::int2(vec![0i16; len]),
					Value::Int4(_) => ColumnBuffer::int4(vec![0i32; len]),
					Value::Int8(_) => ColumnBuffer::int8(vec![0i64; len]),
					Value::Int16(_) => ColumnBuffer::int16(vec![0i128; len]),
					Value::Uint1(_) => ColumnBuffer::uint1(vec![0u8; len]),
					Value::Uint2(_) => ColumnBuffer::uint2(vec![0u16; len]),
					Value::Uint4(_) => ColumnBuffer::uint4(vec![0u32; len]),
					Value::Uint8(_) => ColumnBuffer::uint8(vec![0u64; len]),
					Value::Uint16(_) => ColumnBuffer::uint16(vec![0u128; len]),
					Value::Utf8(_) => ColumnBuffer::utf8(vec![String::new(); len]),
					Value::Date(_) => ColumnBuffer::date(vec![Date::default(); len]),
					Value::DateTime(_) => ColumnBuffer::datetime(vec![DateTime::default(); len]),
					Value::Time(_) => ColumnBuffer::time(vec![Time::default(); len]),
					Value::Duration(_) => ColumnBuffer::duration(vec![Duration::default(); len]),
					Value::Uuid4(_) => ColumnBuffer::uuid4(vec![Uuid4::default(); len]),
					Value::Uuid7(_) => ColumnBuffer::uuid7(vec![Uuid7::default(); len]),
					Value::IdentityId(_) => {
						ColumnBuffer::identity_id(vec![IdentityId::default(); len])
					}
					Value::DictionaryId(_) => {
						ColumnBuffer::dictionary_id(vec![DictionaryEntryId::default(); len])
					}
					Value::Blob(_) => ColumnBuffer::blob(vec![Blob::default(); len]),
					Value::Int(_) => ColumnBuffer::int(vec![Int::default(); len]),
					Value::Uint(_) => ColumnBuffer::uint(vec![Uint::default(); len]),
					Value::Decimal(_) => ColumnBuffer::decimal(vec![Decimal::default(); len]),
					Value::Any(_) => ColumnBuffer::any(vec![Value::none(); len]),
					Value::Record(_) => ColumnBuffer::any(vec![Value::none(); len]),
					Value::Tuple(_) => ColumnBuffer::any(vec![Value::none(); len]),
					Value::List(_) => ColumnBuffer::any(vec![Value::none(); len]),
					Value::Type(_) => ColumnBuffer::any(vec![Value::none(); len]),
					Value::Digest(_) => {
						ColumnBuffer::none_typed(value.get_type(), len).into_unwrap_option().0
					}
					_ => unreachable!(),
				}
				.into_builder();
				new_inner.push_value(value);
				if len > 0 {
					let mut new_bitvec = BooleanBufferBuilder::new(len + 1);
					new_bitvec.append_n(len, false);
					new_bitvec.append(true);
					*self = ColumnBuilder::Option {
						inner: Box::new(new_inner),
						bitvec: new_bitvec,
					};
				} else {
					*self = new_inner;
				}
			} else {
				inner.push_value(value);
				bitvec.append(true);
			}
			return;
		}
		match value {
			Value::Boolean(v) => match self {
				ColumnBuilder::Bool(builder) => builder.append(v),
				_ => unimplemented!(),
			},
			Value::Float4(v) => push_or_promote!(native self, v.value(), Float4),
			Value::Float8(v) => push_or_promote!(native self, v.value(), Float8),
			Value::Int1(v) => push_or_promote!(native self, v, Int1),
			Value::Int2(v) => push_or_promote!(native self, v, Int2),
			Value::Int4(v) => push_or_promote!(native self, v, Int4),
			Value::Int8(v) => push_or_promote!(native self, v, Int8),
			Value::Int16(v) => push_or_promote!(native self, v, Int16),
			Value::Uint1(v) => push_or_promote!(native self, v, Uint1),
			Value::Uint2(v) => push_or_promote!(native self, v, Uint2),
			Value::Uint4(v) => push_or_promote!(native self, v, Uint4),
			Value::Uint8(v) => push_or_promote!(native self, v, Uint8),
			Value::Uint16(v) => push_or_promote!(temporal self, v, Uint16, uint16_to_native),
			Value::Utf8(v) => push_or_promote!(varlen self, v, Utf8),
			Value::Date(v) => push_or_promote!(temporal self, v, Date, date_to_native),
			Value::DateTime(v) => push_or_promote!(temporal self, v, DateTime, datetime_to_native),
			Value::Time(v) => push_or_promote!(temporal self, v, Time, time_to_native),
			Value::Duration(v) => push_or_promote!(temporal self, v, Duration, duration_to_native),
			Value::Uuid4(v) => push_or_promote!(fixed self, v, Uuid4),
			Value::Uuid7(v) => push_or_promote!(fixed self, v, Uuid7),
			Value::IdentityId(v) => push_or_promote!(fixed self, v, IdentityId),
			Value::DictionaryId(v) => match self {
				ColumnBuilder::DictionaryId {
					buffer,
					..
				} => push_entry(buffer, v),
				_ => unimplemented!(),
			},
			Value::Blob(v) => push_or_promote!(varlen self, v.as_bytes(), Blob),
			Value::Int(v) => push_or_promote!(struct_direct self, v, Int),
			Value::Uint(v) => push_or_promote!(struct_direct self, v, Uint),
			Value::Decimal(v) => push_or_promote!(struct_direct self, v, Decimal),
			Value::None {
				..
			} => self.push_none(),
			Value::Type(t) => self.push_value(Value::Any(Box::new(Value::Type(t)))),
			Value::List(v) => self.push_value(Value::Any(Box::new(Value::List(v)))),
			Value::Record(v) => self.push_value(Value::Any(Box::new(Value::Record(v)))),
			Value::Tuple(v) => self.push_value(Value::Any(Box::new(Value::Tuple(v)))),
			Value::Any(v) => match self {
				ColumnBuilder::Buffer(ColumnBuffer::Any(container)) => container.push(*v),
				_ => unreachable!("Cannot push Any value to non-Any column"),
			},
			Value::Digest(digest) => match self {
				ColumnBuilder::Buffer(ColumnBuffer::Digest {
					container,
					inner,
					accuracy,
				}) => {
					if digest.inner() != inner || digest.accuracy() != *accuracy {
						panic!(
							"cannot push a Digest({}, {}) into a Digest({inner}, {accuracy}) column",
							digest.inner(),
							digest.accuracy()
						);
					}
					container.push(digest);
				}
				_ => unimplemented!(),
			},
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use arrow_array::Array;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_value::value::{
		Value,
		container::{
			decimal_array::u128s,
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	};

	use crate::value::column::ColumnBuffer;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_bool() {
		let mut col = ColumnBuffer::bool(vec![true]).into_builder();
		col.push_value(Value::Boolean(false));
		let col = col.finish();
		let ColumnBuffer::Bool(container) = col else {
			panic!("Expected Bool");
		};
		assert_eq!(container.values().iter().collect::<Vec<_>>(), vec![true, false]);
	}

	#[test]
	fn test_undefined_bool() {
		let mut col = ColumnBuffer::bool(vec![true]).into_builder();
		col.push_value(Value::none());
		// Pushing none promotes the bare column to Option-wrapped.
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_bool() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 2).into_builder();
		col.push_value(Value::Boolean(true));
		let col = col.finish();
		assert_eq!(col.len(), 3);
		assert!(!col.is_defined(0));
		assert!(!col.is_defined(1));
		assert!(col.is_defined(2));
		assert_eq!(col.get_value(2), Value::Boolean(true));
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnBuffer::float4(vec![1.0]).into_builder();
		col.push_value(Value::Float4(OrderedF32::try_from(2.0).unwrap()));
		let col = col.finish();
		let ColumnBuffer::Float4(container) = col else {
			panic!("Expected Float4");
		};
		assert_eq!(&container.values()[..], &[1.0, 2.0]);
	}

	#[test]
	fn test_undefined_float4() {
		let mut col = ColumnBuffer::float4(vec![1.0]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_float4() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Float4(OrderedF32::try_from(3.14).unwrap()));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnBuffer::float8(vec![1.0]).into_builder();
		col.push_value(Value::Float8(OrderedF64::try_from(2.0).unwrap()));
		let col = col.finish();
		let ColumnBuffer::Float8(container) = col else {
			panic!("Expected Float8");
		};
		assert_eq!(&container.values()[..], &[1.0, 2.0]);
	}

	#[test]
	fn test_undefined_float8() {
		let mut col = ColumnBuffer::float8(vec![1.0]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_float8() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Float8(OrderedF64::try_from(2.718).unwrap()));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnBuffer::int1(vec![1]).into_builder();
		col.push_value(Value::Int1(2));
		let col = col.finish();
		let ColumnBuffer::Int1(container) = col else {
			panic!("Expected Int1");
		};
		assert_eq!(&container.values()[..], &[1, 2]);
	}

	#[test]
	fn test_undefined_int1() {
		let mut col = ColumnBuffer::int1(vec![1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int1() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Int1(5));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int1(5));
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnBuffer::int2(vec![1]).into_builder();
		col.push_value(Value::Int2(3));
		let col = col.finish();
		let ColumnBuffer::Int2(container) = col else {
			panic!("Expected Int2");
		};
		assert_eq!(&container.values()[..], &[1, 3]);
	}

	#[test]
	fn test_undefined_int2() {
		let mut col = ColumnBuffer::int2(vec![1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int2() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Int2(10));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int2(10));
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnBuffer::int4(vec![10]).into_builder();
		col.push_value(Value::Int4(20));
		let col = col.finish();
		let ColumnBuffer::Int4(container) = col else {
			panic!("Expected Int4");
		};
		assert_eq!(&container.values()[..], &[10, 20]);
	}

	#[test]
	fn test_undefined_int4() {
		let mut col = ColumnBuffer::int4(vec![10]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int4() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Int4(20));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int4(20));
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnBuffer::int8(vec![100]).into_builder();
		col.push_value(Value::Int8(200));
		let col = col.finish();
		let ColumnBuffer::Int8(container) = col else {
			panic!("Expected Int8");
		};
		assert_eq!(&container.values()[..], &[100, 200]);
	}

	#[test]
	fn test_undefined_int8() {
		let mut col = ColumnBuffer::int8(vec![100]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int8() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Int8(30));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int8(30));
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnBuffer::int16(vec![1000]).into_builder();
		col.push_value(Value::Int16(2000));
		let col = col.finish();
		let ColumnBuffer::Int16(container) = col else {
			panic!("Expected Int16");
		};
		assert_eq!(&container.values()[..], &[1000, 2000]);
	}

	#[test]
	fn test_undefined_int16() {
		let mut col = ColumnBuffer::int16(vec![1000]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int16() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Int16(40));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int16(40));
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnBuffer::uint1(vec![1]).into_builder();
		col.push_value(Value::Uint1(2));
		let col = col.finish();
		let ColumnBuffer::Uint1(container) = col else {
			panic!("Expected Uint1");
		};
		assert_eq!(&container.values()[..], &[1, 2]);
	}

	#[test]
	fn test_undefined_uint1() {
		let mut col = ColumnBuffer::uint1(vec![1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint1() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uint1(1));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint1(1));
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnBuffer::uint2(vec![10]).into_builder();
		col.push_value(Value::Uint2(20));
		let col = col.finish();
		let ColumnBuffer::Uint2(container) = col else {
			panic!("Expected Uint2");
		};
		assert_eq!(&container.values()[..], &[10, 20]);
	}

	#[test]
	fn test_undefined_uint2() {
		let mut col = ColumnBuffer::uint2(vec![10]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint2() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uint2(2));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint2(2));
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnBuffer::uint4(vec![100]).into_builder();
		col.push_value(Value::Uint4(200));
		let col = col.finish();
		let ColumnBuffer::Uint4(container) = col else {
			panic!("Expected Uint4");
		};
		assert_eq!(&container.values()[..], &[100, 200]);
	}

	#[test]
	fn test_undefined_uint4() {
		let mut col = ColumnBuffer::uint4(vec![100]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint4() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uint4(3));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint4(3));
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnBuffer::uint8(vec![1000]).into_builder();
		col.push_value(Value::Uint8(2000));
		let col = col.finish();
		let ColumnBuffer::Uint8(container) = col else {
			panic!("Expected Uint8");
		};
		assert_eq!(&container.values()[..], &[1000, 2000]);
	}

	#[test]
	fn test_undefined_uint8() {
		let mut col = ColumnBuffer::uint8(vec![1000]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint8() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uint8(4));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint8(4));
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnBuffer::uint16(vec![10000]).into_builder();
		col.push_value(Value::Uint16(20000));
		let col = col.finish();
		let ColumnBuffer::Uint16(container) = col else {
			panic!("Expected Uint16");
		};
		assert_eq!(u128s(&container), &[10000, 20000]);
	}

	#[test]
	fn test_undefined_uint16() {
		let mut col = ColumnBuffer::uint16(vec![10000]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint16() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uint16(5));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint16(5));
	}

	#[test]
	fn test_utf8() {
		let mut col = ColumnBuffer::utf8(vec!["hello".to_string()]).into_builder();
		col.push_value(Value::Utf8("world".to_string()));
		let col = col.finish();
		let ColumnBuffer::Utf8 {
			container,
			..
		} = col
		else {
			panic!("Expected Utf8");
		};
		let collected: Vec<&str> = (0..container.len()).map(|i| container.value(i)).collect();
		assert_eq!(collected, vec!["hello", "world"]);
	}

	#[test]
	fn test_undefined_utf8() {
		let mut col = ColumnBuffer::utf8(vec!["hello".to_string()]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_utf8() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Utf8("ok".to_string()));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Utf8("ok".to_string()));
	}

	#[test]
	fn test_undefined() {
		let mut col = ColumnBuffer::int2(vec![1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let date2 = Date::from_ymd(2023, 12, 31).unwrap();
		let mut col = ColumnBuffer::date(vec![date1]).into_builder();
		col.push_value(Value::Date(date2));
		let col = col.finish();
		let ColumnBuffer::Date(container) = col else {
			panic!("Expected Date");
		};
		assert_eq!(dates(&container), &[date1, date2]);
	}

	#[test]
	fn test_undefined_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let mut col = ColumnBuffer::date(vec![date1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_date() {
		let date = Date::from_ymd(2023, 6, 15).unwrap();
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Date(date));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Date(date));
	}

	#[test]
	fn test_datetime() {
		let dt1 = DateTime::from_epoch_secs(1672531200).unwrap(); // 2023-01-01 00:00:00 SVTC
		let dt2 = DateTime::from_epoch_secs(1704067200).unwrap(); // 2024-01-01 00:00:00 SVTC
		let mut col = ColumnBuffer::datetime(vec![dt1]).into_builder();
		col.push_value(Value::DateTime(dt2));
		let col = col.finish();
		let ColumnBuffer::DateTime(container) = col else {
			panic!("Expected DateTime");
		};
		assert_eq!(datetimes(&container), &[dt1, dt2]);
	}

	#[test]
	fn test_undefined_datetime() {
		let dt1 = DateTime::from_epoch_secs(1672531200).unwrap();
		let mut col = ColumnBuffer::datetime(vec![dt1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_datetime() {
		let dt = DateTime::from_epoch_secs(1672531200).unwrap();
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::DateTime(dt));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::DateTime(dt));
	}

	#[test]
	fn test_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let time2 = Time::from_hms(18, 45, 30).unwrap();
		let mut col = ColumnBuffer::time(vec![time1]).into_builder();
		col.push_value(Value::Time(time2));
		let col = col.finish();
		let ColumnBuffer::Time(container) = col else {
			panic!("Expected Time");
		};
		assert_eq!(times(&container), &[time1, time2]);
	}

	#[test]
	fn test_undefined_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let mut col = ColumnBuffer::time(vec![time1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_time() {
		let time = Time::from_hms(15, 20, 10).unwrap();
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Time(time));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Time(time));
	}

	#[test]
	fn test_duration() {
		let duration1 = Duration::from_days(30).unwrap();
		let duration2 = Duration::from_hours(24).unwrap();
		let mut col = ColumnBuffer::duration(vec![duration1]).into_builder();
		col.push_value(Value::Duration(duration2));
		let col = col.finish();
		let ColumnBuffer::Duration(container) = col else {
			panic!("Expected Duration");
		};
		assert_eq!(durations(&container), &[duration1, duration2]);
	}

	#[test]
	fn test_undefined_duration() {
		let duration1 = Duration::from_days(30).unwrap();
		let mut col = ColumnBuffer::duration(vec![duration1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_duration() {
		let duration = Duration::from_minutes(90).unwrap();
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Duration(duration));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Duration(duration));
	}

	#[test]
	fn test_identity_id() {
		let (mock, clock, rng) = test_clock_and_rng();
		let id1 = IdentityId::generate(&clock, &rng);
		mock.advance_millis(1);
		let id2 = IdentityId::generate(&clock, &rng);
		let mut col = ColumnBuffer::identity_id(vec![id1]).into_builder();
		col.push_value(Value::IdentityId(id2));
		let col = col.finish();
		let ColumnBuffer::IdentityId(container) = col else {
			panic!("Expected IdentityId");
		};
		assert_eq!(identity_ids(&container), &[id1, id2]);
	}

	#[test]
	fn test_undefined_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let id1 = IdentityId::generate(&clock, &rng);
		let mut col = ColumnBuffer::identity_id(vec![id1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = IdentityId::generate(&clock, &rng);
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::IdentityId(id));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::IdentityId(id));
	}

	#[test]
	fn test_uuid4() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let mut col = ColumnBuffer::uuid4(vec![uuid1]).into_builder();
		col.push_value(Value::Uuid4(uuid2));
		let col = col.finish();
		let ColumnBuffer::Uuid4(container) = col else {
			panic!("Expected Uuid4");
		};
		assert_eq!(uuid4s(&container), &[uuid1, uuid2]);
	}

	#[test]
	fn test_undefined_uuid4() {
		let uuid1 = Uuid4::generate();
		let mut col = ColumnBuffer::uuid4(vec![uuid1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uuid4() {
		let uuid = Uuid4::generate();
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uuid4(uuid));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uuid4(uuid));
	}

	#[test]
	fn test_uuid7() {
		let (mock, clock, rng) = test_clock_and_rng();
		let uuid1 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid2 = Uuid7::generate(&clock, &rng);
		let mut col = ColumnBuffer::uuid7(vec![uuid1]).into_builder();
		col.push_value(Value::Uuid7(uuid2));
		let col = col.finish();
		let ColumnBuffer::Uuid7(container) = col else {
			panic!("Expected Uuid7");
		};
		assert_eq!(uuid7s(&container), &[uuid1, uuid2]);
	}

	#[test]
	fn test_undefined_uuid7() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid1 = Uuid7::generate(&clock, &rng);
		let mut col = ColumnBuffer::uuid7(vec![uuid1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uuid7() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid = Uuid7::generate(&clock, &rng);
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Uuid7(uuid));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uuid7(uuid));
	}

	#[test]
	fn test_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);
		let mut col = ColumnBuffer::dictionary_id(vec![e1]).into_builder();
		col.push_value(Value::DictionaryId(e2));
		let col = col.finish();
		let ColumnBuffer::DictionaryId {
			container,
			..
		} = col
		else {
			panic!("Expected DictionaryId");
		};
		assert_eq!(dictionary_array::iter(&container).collect::<Vec<_>>(), &[e1, e2]);
	}

	#[test]
	fn test_undefined_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let mut col = ColumnBuffer::dictionary_id(vec![e1]).into_builder();
		col.push_value(Value::none());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_dictionary_id() {
		let e = DictionaryEntryId::U4(42);
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::DictionaryId(e));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::DictionaryId(e));
	}

	#[test]
	fn test_push_value_to_none_list() {
		// A list arriving after only none rows must widen the column like a record does, never hit an
		// unreachable arm.
		let list = Value::List(vec![Value::Int4(1), Value::Int4(2)]);
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(list.clone());
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Any(Box::new(list)));
	}

	#[test]
	fn test_push_value_to_none_type() {
		// A type value arriving after only none rows must widen the column like a record does, never hit an
		// unreachable arm.
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 1).into_builder();
		col.push_value(Value::Type(ValueType::Int4));
		let col = col.finish();
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Any(Box::new(Value::Type(ValueType::Int4))));
	}
}
