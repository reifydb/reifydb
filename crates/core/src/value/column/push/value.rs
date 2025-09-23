use reifydb_type::Value;

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use crate::value::column::data::ColumnData;

impl ColumnData {
	pub fn push_value(&mut self, value: Value) {
		match value {
			Value::Boolean(v) => match self {
				ColumnData::Bool(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::bool(vec![]);
					if let ColumnData::Bool(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Float4(v) => match self {
				ColumnData::Float4(_) => self.push(v.value()),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::float4(vec![]);
					if let ColumnData::Float4(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v.value());
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Float8(v) => match self {
				ColumnData::Float8(_) => self.push(v.value()),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::float8(vec![]);
					if let ColumnData::Float8(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v.value());
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Int1(v) => match self {
				ColumnData::Int1(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::int1(vec![]);
					if let ColumnData::Int1(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Int2(v) => match self {
				ColumnData::Int2(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::int2(vec![]);
					if let ColumnData::Int2(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Int4(v) => match self {
				ColumnData::Int4(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::int4(vec![]);
					if let ColumnData::Int4(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Int8(v) => match self {
				ColumnData::Int8(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::int8(vec![]);
					if let ColumnData::Int8(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Int16(v) => match self {
				ColumnData::Int16(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::int16(vec![]);
					if let ColumnData::Int16(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Utf8(v) => match self {
				ColumnData::Utf8 {
					..
				} => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::utf8(Vec::<String>::new());
					if let ColumnData::Utf8 {
						container: new_container,
						..
					} = &mut new_container
					{
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uint1(v) => match self {
				ColumnData::Uint1(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uint1(vec![]);
					if let ColumnData::Uint1(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uint2(v) => match self {
				ColumnData::Uint2(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uint2(vec![]);
					if let ColumnData::Uint2(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uint4(v) => match self {
				ColumnData::Uint4(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uint4(vec![]);
					if let ColumnData::Uint4(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uint8(v) => match self {
				ColumnData::Uint8(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uint8(vec![]);
					if let ColumnData::Uint8(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uint16(v) => match self {
				ColumnData::Uint16(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uint16(vec![]);
					if let ColumnData::Uint16(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Date(v) => match self {
				ColumnData::Date(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::date(vec![]);
					if let ColumnData::Date(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::DateTime(v) => match self {
				ColumnData::DateTime(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::datetime(vec![]);
					if let ColumnData::DateTime(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Time(v) => match self {
				ColumnData::Time(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::time(vec![]);
					if let ColumnData::Time(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Interval(v) => match self {
				ColumnData::Interval(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::interval(vec![]);
					if let ColumnData::Interval(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uuid4(v) => match self {
				ColumnData::Uuid4(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uuid4(vec![]);
					if let ColumnData::Uuid4(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Uuid7(v) => match self {
				ColumnData::Uuid7(_) => self.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uuid7(vec![]);
					if let ColumnData::Uuid7(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Undefined => self.push_undefined(),
			Value::RowNumber(row_number) => match self {
				ColumnData::RowNumber(container) => container.push(row_number),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::row_number(vec![]);
					if let ColumnData::RowNumber(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(row_number);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},
			Value::IdentityId(id) => match self {
				ColumnData::IdentityId(container) => container.push(id),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::identity_id(vec![]);
					if let ColumnData::IdentityId(new_container) = &mut new_container {
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(id);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},
			Value::Blob(v) => match self {
				ColumnData::Blob {
					container,
					..
				} => container.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::blob(vec![]);
					if let ColumnData::Blob {
						container: new_container,
						..
					} = &mut new_container
					{
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Int(v) => match self {
				ColumnData::Int {
					container,
					..
				} => container.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::int(vec![]);
					if let ColumnData::Int {
						container: new_container,
						..
					} = &mut new_container
					{
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},
			Value::Uint(v) => match self {
				ColumnData::Uint {
					container,
					..
				} => container.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::uint(vec![]);
					if let ColumnData::Uint {
						container: new_container,
						..
					} = &mut new_container
					{
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},

			Value::Decimal(v) => match self {
				ColumnData::Decimal {
					container,
					..
				} => container.push(v),
				ColumnData::Undefined(container) => {
					let mut new_container = ColumnData::decimal(vec![]);
					if let ColumnData::Decimal {
						container: new_container,
						..
					} = &mut new_container
					{
						for _ in 0..container.len() {
							new_container.push_undefined();
						}
						new_container.push(v);
					}
					*self = new_container;
				}
				_ => unimplemented!(),
			},
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
	use reifydb_type::{
		Date, DateTime, IdentityId, Interval, OrderedF32, OrderedF64, RowNumber, Time, Uuid4, Uuid7, Value,
	};
	use uuid::Uuid;

	use crate::value::column::ColumnData;

	#[test]
	fn test_bool() {
		let mut col = ColumnData::bool(vec![true]);
		col.push_value(Value::Boolean(false));
		let ColumnData::Bool(container) = col else {
			panic!("Expected Bool");
		};
		assert_eq!(container.data().to_vec(), vec![true, false]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_bool() {
		let mut col = ColumnData::bool(vec![true]);
		col.push_value(Value::Undefined);
		let ColumnData::Bool(container) = col else {
			panic!("Expected Bool");
		};
		assert_eq!(container.data().to_vec(), vec![true, false]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_bool() {
		let mut col = ColumnData::undefined(2);
		col.push_value(Value::Boolean(true));
		let ColumnData::Bool(container) = col else {
			panic!("Expected Bool");
		};
		assert_eq!(container.data().to_vec(), vec![false, false, true]);
		assert_eq!(container.bitvec().to_vec(), vec![false, false, true]);
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnData::float4(vec![1.0]);
		col.push_value(Value::Float4(OrderedF32::try_from(2.0).unwrap()));
		let ColumnData::Float4(container) = col else {
			panic!("Expected Float4");
		};
		assert_eq!(container.data().as_slice(), &[1.0, 2.0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_float4() {
		let mut col = ColumnData::float4(vec![1.0]);
		col.push_value(Value::Undefined);
		let ColumnData::Float4(container) = col else {
			panic!("Expected Float4");
		};
		assert_eq!(container.data().as_slice(), &[1.0, 0.0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_float4() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Float4(OrderedF32::try_from(3.14).unwrap()));
		let ColumnData::Float4(container) = col else {
			panic!("Expected Float4");
		};
		assert_eq!(container.data().as_slice(), &[0.0, 3.14]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnData::float8(vec![1.0]);
		col.push_value(Value::Float8(OrderedF64::try_from(2.0).unwrap()));
		let ColumnData::Float8(container) = col else {
			panic!("Expected Float8");
		};
		assert_eq!(container.data().as_slice(), &[1.0, 2.0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_float8() {
		let mut col = ColumnData::float8(vec![1.0]);
		col.push_value(Value::Undefined);
		let ColumnData::Float8(container) = col else {
			panic!("Expected Float8");
		};
		assert_eq!(container.data().as_slice(), &[1.0, 0.0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_float8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Float8(OrderedF64::try_from(2.718).unwrap()));
		let ColumnData::Float8(container) = col else {
			panic!("Expected Float8");
		};
		assert_eq!(container.data().as_slice(), &[0.0, 2.718]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnData::int1(vec![1]);
		col.push_value(Value::Int1(2));
		let ColumnData::Int1(container) = col else {
			panic!("Expected Int1");
		};
		assert_eq!(container.data().as_slice(), &[1, 2]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_int1() {
		let mut col = ColumnData::int1(vec![1]);
		col.push_value(Value::Undefined);
		let ColumnData::Int1(container) = col else {
			panic!("Expected Int1");
		};
		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_int1() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int1(5));
		let ColumnData::Int1(container) = col else {
			panic!("Expected Int1");
		};
		assert_eq!(container.data().as_slice(), &[0, 5]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_value(Value::Int2(3));
		let ColumnData::Int2(container) = col else {
			panic!("Expected Int2");
		};
		assert_eq!(container.data().as_slice(), &[1, 3]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_int2() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_value(Value::Undefined);
		let ColumnData::Int2(container) = col else {
			panic!("Expected Int2");
		};
		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_int2() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int2(10));
		let ColumnData::Int2(container) = col else {
			panic!("Expected Int2");
		};
		assert_eq!(container.data().as_slice(), &[0, 10]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnData::int4(vec![10]);
		col.push_value(Value::Int4(20));
		let ColumnData::Int4(container) = col else {
			panic!("Expected Int4");
		};
		assert_eq!(container.data().as_slice(), &[10, 20]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_int4() {
		let mut col = ColumnData::int4(vec![10]);
		col.push_value(Value::Undefined);
		let ColumnData::Int4(container) = col else {
			panic!("Expected Int4");
		};
		assert_eq!(container.data().as_slice(), &[10, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_int4() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int4(20));
		let ColumnData::Int4(container) = col else {
			panic!("Expected Int4");
		};
		assert_eq!(container.data().as_slice(), &[0, 20]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnData::int8(vec![100]);
		col.push_value(Value::Int8(200));
		let ColumnData::Int8(container) = col else {
			panic!("Expected Int8");
		};
		assert_eq!(container.data().as_slice(), &[100, 200]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_int8() {
		let mut col = ColumnData::int8(vec![100]);
		col.push_value(Value::Undefined);
		let ColumnData::Int8(container) = col else {
			panic!("Expected Int8");
		};
		assert_eq!(container.data().as_slice(), &[100, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_int8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int8(30));
		let ColumnData::Int8(container) = col else {
			panic!("Expected Int8");
		};
		assert_eq!(container.data().as_slice(), &[0, 30]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnData::int16(vec![1000]);
		col.push_value(Value::Int16(2000));
		let ColumnData::Int16(container) = col else {
			panic!("Expected Int16");
		};
		assert_eq!(container.data().as_slice(), &[1000, 2000]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_int16() {
		let mut col = ColumnData::int16(vec![1000]);
		col.push_value(Value::Undefined);
		let ColumnData::Int16(container) = col else {
			panic!("Expected Int16");
		};
		assert_eq!(container.data().as_slice(), &[1000, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_int16() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Int16(40));
		let ColumnData::Int16(container) = col else {
			panic!("Expected Int16");
		};
		assert_eq!(container.data().as_slice(), &[0, 40]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnData::uint1(vec![1]);
		col.push_value(Value::Uint1(2));
		let ColumnData::Uint1(container) = col else {
			panic!("Expected Uint1");
		};
		assert_eq!(container.data().as_slice(), &[1, 2]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uint1() {
		let mut col = ColumnData::uint1(vec![1]);
		col.push_value(Value::Undefined);
		let ColumnData::Uint1(container) = col else {
			panic!("Expected Uint1");
		};
		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uint1() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint1(1));
		let ColumnData::Uint1(container) = col else {
			panic!("Expected Uint1");
		};
		assert_eq!(container.data().as_slice(), &[0, 1]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnData::uint2(vec![10]);
		col.push_value(Value::Uint2(20));
		let ColumnData::Uint2(container) = col else {
			panic!("Expected Uint2");
		};
		assert_eq!(container.data().as_slice(), &[10, 20]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uint2() {
		let mut col = ColumnData::uint2(vec![10]);
		col.push_value(Value::Undefined);
		let ColumnData::Uint2(container) = col else {
			panic!("Expected Uint2");
		};
		assert_eq!(container.data().as_slice(), &[10, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uint2() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint2(2));
		let ColumnData::Uint2(container) = col else {
			panic!("Expected Uint2");
		};
		assert_eq!(container.data().as_slice(), &[0, 2]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnData::uint4(vec![100]);
		col.push_value(Value::Uint4(200));
		let ColumnData::Uint4(container) = col else {
			panic!("Expected Uint4");
		};
		assert_eq!(container.data().as_slice(), &[100, 200]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uint4() {
		let mut col = ColumnData::uint4(vec![100]);
		col.push_value(Value::Undefined);
		let ColumnData::Uint4(container) = col else {
			panic!("Expected Uint4");
		};
		assert_eq!(container.data().as_slice(), &[100, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uint4() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint4(3));
		let ColumnData::Uint4(container) = col else {
			panic!("Expected Uint4");
		};
		assert_eq!(container.data().as_slice(), &[0, 3]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnData::uint8(vec![1000]);
		col.push_value(Value::Uint8(2000));
		let ColumnData::Uint8(container) = col else {
			panic!("Expected Uint8");
		};
		assert_eq!(container.data().as_slice(), &[1000, 2000]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uint8() {
		let mut col = ColumnData::uint8(vec![1000]);
		col.push_value(Value::Undefined);
		let ColumnData::Uint8(container) = col else {
			panic!("Expected Uint8");
		};
		assert_eq!(container.data().as_slice(), &[1000, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uint8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint8(4));
		let ColumnData::Uint8(container) = col else {
			panic!("Expected Uint8");
		};
		assert_eq!(container.data().as_slice(), &[0, 4]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnData::uint16(vec![10000]);
		col.push_value(Value::Uint16(20000));
		let ColumnData::Uint16(container) = col else {
			panic!("Expected Uint16");
		};
		assert_eq!(container.data().as_slice(), &[10000, 20000]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uint16() {
		let mut col = ColumnData::uint16(vec![10000]);
		col.push_value(Value::Undefined);
		let ColumnData::Uint16(container) = col else {
			panic!("Expected Uint16");
		};
		assert_eq!(container.data().as_slice(), &[10000, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uint16() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uint16(5));
		let ColumnData::Uint16(container) = col else {
			panic!("Expected Uint16");
		};
		assert_eq!(container.data().as_slice(), &[0, 5]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_utf8() {
		let mut col = ColumnData::utf8(vec!["hello".to_string()]);
		col.push_value(Value::Undefined);
		let ColumnData::Utf8 {
			container,
			..
		} = col
		else {
			panic!("Expected Utf8");
		};
		assert_eq!(container.data().as_slice(), &["hello".to_string(), "".to_string()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_utf8() {
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Utf8("ok".to_string()));
		let ColumnData::Utf8 {
			container,
			..
		} = col
		else {
			panic!("Expected Utf8");
		};
		assert_eq!(container.data().as_slice(), &["".to_string(), "ok".to_string()]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_undefined() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_value(Value::Undefined);
		let ColumnData::Int2(container) = col else {
			panic!("Expected Int2");
		};
		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let mut col = ColumnData::date(vec![date1]);
		col.push_value(Value::Undefined);
		let ColumnData::Date(container) = col else {
			panic!("Expected Date");
		};
		assert_eq!(container.data().as_slice(), &[date1, Date::default()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_date() {
		let date = Date::from_ymd(2023, 6, 15).unwrap();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Date(date));
		let ColumnData::Date(container) = col else {
			panic!("Expected Date");
		};
		assert_eq!(container.data().as_slice(), &[Date::default(), date]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_datetime() {
		use DateTime;
		let dt1 = DateTime::from_timestamp(1672531200).unwrap();
		let mut col = ColumnData::datetime(vec![dt1]);
		col.push_value(Value::Undefined);
		let ColumnData::DateTime(container) = col else {
			panic!("Expected DateTime");
		};
		assert_eq!(container.data().as_slice(), &[dt1, DateTime::default()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_datetime() {
		let dt = DateTime::from_timestamp(1672531200).unwrap();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::DateTime(dt));
		let ColumnData::DateTime(container) = col else {
			panic!("Expected DateTime");
		};
		assert_eq!(container.data().as_slice(), &[DateTime::default(), dt]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let mut col = ColumnData::time(vec![time1]);
		col.push_value(Value::Undefined);
		let ColumnData::Time(container) = col else {
			panic!("Expected Time");
		};
		assert_eq!(container.data().as_slice(), &[time1, Time::default()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_time() {
		let time = Time::from_hms(15, 20, 10).unwrap();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Time(time));
		let ColumnData::Time(container) = col else {
			panic!("Expected Time");
		};
		assert_eq!(container.data().as_slice(), &[Time::default(), time]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_interval() {
		let interval1 = Interval::from_days(30);
		let interval2 = Interval::from_hours(24);
		let mut col = ColumnData::interval(vec![interval1]);
		col.push_value(Value::Interval(interval2));
		let ColumnData::Interval(container) = col else {
			panic!("Expected Interval");
		};
		assert_eq!(container.data().as_slice(), &[interval1, interval2]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_interval() {
		let interval1 = Interval::from_days(30);
		let mut col = ColumnData::interval(vec![interval1]);
		col.push_value(Value::Undefined);
		let ColumnData::Interval(container) = col else {
			panic!("Expected Interval");
		};
		assert_eq!(container.data().as_slice(), &[interval1, Interval::default()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_interval() {
		let interval = Interval::from_minutes(90);
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Interval(interval));
		let ColumnData::Interval(container) = col else {
			panic!("Expected Interval");
		};
		assert_eq!(container.data().as_slice(), &[Interval::default(), interval]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}

	#[test]
	fn test_row_number() {
		let row_number1 = RowNumber::new(1);
		let row_number2 = RowNumber::new(2);
		let mut col = ColumnData::row_number(vec![row_number1]);
		col.push_value(Value::RowNumber(row_number2));
		let ColumnData::RowNumber(container) = col else {
			panic!("Expected RowNumber");
		};
		assert_eq!(container.data().as_slice(), &[row_number1, row_number2]);
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_row_number() {
		let row_number1 = RowNumber::new(1);
		let mut col = ColumnData::row_number(vec![row_number1]);
		col.push_value(Value::Undefined);
		let ColumnData::RowNumber(container) = col else {
			panic!("Expected RowNumber");
		};
		assert_eq!(container.data().as_slice(), &[row_number1, RowNumber::default()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_row_number() {
		let row_number = RowNumber::new(42);
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::RowNumber(row_number));
		let ColumnData::RowNumber(container) = col else {
			panic!("Expected RowNumber");
		};
		assert_eq!(container.data().as_slice(), &[RowNumber::default(), row_number]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_identity_id() {
		let id1 = IdentityId::generate();
		let mut col = ColumnData::identity_id(vec![id1]);
		col.push_value(Value::Undefined);
		let ColumnData::IdentityId(container) = col else {
			panic!("Expected IdentityId");
		};
		assert_eq!(container.data().as_slice(), &[id1, IdentityId::default()]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_identity_id() {
		let id = IdentityId::generate();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::IdentityId(id));
		let ColumnData::IdentityId(container) = col else {
			panic!("Expected IdentityId");
		};
		assert_eq!(container.data().as_slice(), &[IdentityId::default(), id]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uuid4() {
		let uuid1 = Uuid4::generate();
		let mut col = ColumnData::uuid4(vec![uuid1]);
		col.push_value(Value::Undefined);
		let ColumnData::Uuid4(container) = col else {
			panic!("Expected Uuid4");
		};
		assert_eq!(container.data().as_slice(), &[uuid1, Uuid4::from(Uuid::nil())]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uuid4() {
		let uuid = Uuid4::generate();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uuid4(uuid));
		let ColumnData::Uuid4(container) = col else {
			panic!("Expected Uuid4");
		};
		assert_eq!(container.data().as_slice(), &[Uuid4::from(Uuid::nil()), uuid]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
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
		assert_eq!(container.bitvec().to_vec(), vec![true, true]);
	}

	#[test]
	fn test_undefined_uuid7() {
		let uuid1 = Uuid7::generate();
		let mut col = ColumnData::uuid7(vec![uuid1]);
		col.push_value(Value::Undefined);
		let ColumnData::Uuid7(container) = col else {
			panic!("Expected Uuid7");
		};
		assert_eq!(container.data().as_slice(), &[uuid1, Uuid7::from(Uuid::nil())]);
		assert_eq!(container.bitvec().to_vec(), vec![true, false]);
	}

	#[test]
	fn test_push_value_to_undefined_uuid7() {
		let uuid = Uuid7::generate();
		let mut col = ColumnData::undefined(1);
		col.push_value(Value::Uuid7(uuid));
		let ColumnData::Uuid7(container) = col else {
			panic!("Expected Uuid7");
		};
		assert_eq!(container.data().as_slice(), &[Uuid7::from(Uuid::nil()), uuid]);
		assert_eq!(container.bitvec().to_vec(), vec![false, true]);
	}
}
