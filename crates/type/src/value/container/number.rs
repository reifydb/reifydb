// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	any::TypeId,
	fmt::{self, Debug},
	mem::{forget, transmute_copy},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::cowvec::CowVec,
	value::{
		Value,
		Value::{Int1, Int2, Int4, Int8, Int16, Uint1, Uint2, Uint4, Uint8, Uint16},
		decimal::Decimal,
		int::Int,
		is::IsNumber,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		r#type::Type,
		uint::Uint,
	},
};

pub struct NumberContainer<T, S: Storage = Cow>
where
	T: IsNumber,
{
	data: S::Vec<T>,
}

impl<T: IsNumber, S: Storage> Clone for NumberContainer<T, S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<T: IsNumber + Debug, S: Storage> Debug for NumberContainer<T, S>
where
	S::Vec<T>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("NumberContainer").field("data", &self.data).finish()
	}
}

impl<T: IsNumber, S: Storage> PartialEq for NumberContainer<T, S>
where
	S::Vec<T>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl<T: IsNumber + Serialize> Serialize for NumberContainer<T, Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a, T: Clone + PartialEq + Serialize> {
			data: &'a CowVec<T>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de, T: IsNumber + Deserialize<'de>> Deserialize<'de> for NumberContainer<T, Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: CowVec<T>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(NumberContainer {
			data: h.data,
		})
	}
}

impl<T: IsNumber, S: Storage> Deref for NumberContainer<T, S> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> NumberContainer<T, Cow>
where
	T: IsNumber + Clone + Debug + Default,
{
	pub fn new(data: Vec<T>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<T>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}
}

impl<T, S: Storage> NumberContainer<T, S>
where
	T: IsNumber + Clone + Debug + Default,
{
	pub fn from_parts(data: S::Vec<T>) -> Self {
		Self {
			data,
		}
	}

	pub fn len(&self) -> usize {
		DataVec::len(&self.data)
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data)
	}

	pub fn is_empty(&self) -> bool {
		DataVec::is_empty(&self.data)
	}

	pub fn clear(&mut self) {
		DataVec::clear(&mut self.data);
	}

	pub fn push(&mut self, value: T) {
		DataVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataVec::push(&mut self.data, T::default());
	}

	pub fn get(&self, index: usize) -> Option<&T> {
		if index < self.len() {
			DataVec::get(&self.data, index)
		} else {
			None
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn is_fully_defined(&self) -> bool {
		true
	}

	pub fn data(&self) -> &S::Vec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<T> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			self.data[index].to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value
	where
		T: 'static,
	{
		if index < self.len() {
			let value = self.data[index].clone();

			if TypeId::of::<T>() == TypeId::of::<f32>() {
				let f_val = unsafe { transmute_copy::<T, f32>(&value) };
				OrderedF32::try_from(f_val).map(Value::Float4).unwrap_or(Value::None { inner: Type::Float4 })
			} else if TypeId::of::<T>() == TypeId::of::<f64>() {
				let f_val = unsafe { transmute_copy::<T, f64>(&value) };
				OrderedF64::try_from(f_val).map(Value::Float8).unwrap_or(Value::None { inner: Type::Float8 })
			} else if TypeId::of::<T>() == TypeId::of::<i8>() {
				let i_val = unsafe { transmute_copy::<T, i8>(&value) };
				Int1(i_val)
			} else if TypeId::of::<T>() == TypeId::of::<i16>() {
				let i_val = unsafe { transmute_copy::<T, i16>(&value) };
				Int2(i_val)
			} else if TypeId::of::<T>() == TypeId::of::<i32>() {
				let i_val = unsafe { transmute_copy::<T, i32>(&value) };
				Int4(i_val)
			} else if TypeId::of::<T>() == TypeId::of::<i64>() {
				let i_val = unsafe { transmute_copy::<T, i64>(&value) };
				Int8(i_val)
			} else if TypeId::of::<T>() == TypeId::of::<i128>() {
				let i_val = unsafe { transmute_copy::<T, i128>(&value) };
				Int16(i_val)
			} else if TypeId::of::<T>() == TypeId::of::<u8>() {
				let u_val = unsafe { transmute_copy::<T, u8>(&value) };
				Uint1(u_val)
			} else if TypeId::of::<T>() == TypeId::of::<u16>() {
				let u_val = unsafe { transmute_copy::<T, u16>(&value) };
				Uint2(u_val)
			} else if TypeId::of::<T>() == TypeId::of::<u32>() {
				let u_val = unsafe { transmute_copy::<T, u32>(&value) };
				Uint4(u_val)
			} else if TypeId::of::<T>() == TypeId::of::<u64>() {
				let u_val = unsafe { transmute_copy::<T, u64>(&value) };
				Uint8(u_val)
			} else if TypeId::of::<T>() == TypeId::of::<u128>() {
				let u_val = unsafe { transmute_copy::<T, u128>(&value) };
				Uint16(u_val)
			} else if TypeId::of::<T>() == TypeId::of::<Decimal>() {
				let d_val = unsafe { transmute_copy::<T, Decimal>(&value) };
				forget(value);
				Value::Decimal(d_val)
			} else if TypeId::of::<T>() == TypeId::of::<Int>() {
				let i_val = unsafe { transmute_copy::<T, Int>(&value) };
				forget(value);
				Value::Int(i_val)
			} else if TypeId::of::<T>() == TypeId::of::<Uint>() {
				let u_val = unsafe { transmute_copy::<T, Uint>(&value) };
				forget(value);
				Value::Uint(u_val)
			} else {
				Value::none()
			}
		} else {
			Value::none()
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_
	where
		T: Copy,
	{
		self.data.iter().map(|&v| Some(v))
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataVec::spawn(&self.data, count);
		for i in start..(start + count) {
			DataVec::push(&mut new_data, self.data[i].clone());
		}
		Self {
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataVec::push(&mut new_data, self.data[i].clone());
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataVec::spawn(&self.data, indices.len());

		for &idx in indices {
			if idx < self.len() {
				DataVec::push(&mut new_data, self.data[idx].clone());
			} else {
				DataVec::push(&mut new_data, T::default());
			}
		}

		self.data = new_data;
	}

	pub fn push_with_convert<U>(&mut self, value: U, converter: impl FnOnce(U) -> Option<T>) {
		match converter(value) {
			Some(v) => {
				DataVec::push(&mut self.data, v);
			}
			None => {
				DataVec::push(&mut self.data, T::default());
			}
		}
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_new_i32() {
		let data = vec![1, 2, 3];
		let container = NumberContainer::new(data.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&1));
		assert_eq!(container.get(1), Some(&2));
		assert_eq!(container.get(2), Some(&3));
	}

	#[test]
	fn test_from_vec_f64() {
		let data = vec![1.1, 2.2, 3.3];
		let container = NumberContainer::from_vec(data);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&1.1));
		assert_eq!(container.get(1), Some(&2.2));
		assert_eq!(container.get(2), Some(&3.3));

		// All should be defined
		for i in 0..3 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container: NumberContainer<i32> = NumberContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_i64() {
		let mut container: NumberContainer<i64> = NumberContainer::with_capacity(3);

		container.push(100);
		container.push(-200);
		container.push_default();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&100));
		assert_eq!(container.get(1), Some(&-200));
		assert_eq!(container.get(2), Some(&0)); // push_default pushes default

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let mut container1 = NumberContainer::from_vec(vec![1i32, 2]);
		let container2 = NumberContainer::from_vec(vec![3i32, 4]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 4);
		assert_eq!(container1.get(0), Some(&1));
		assert_eq!(container1.get(1), Some(&2));
		assert_eq!(container1.get(2), Some(&3));
		assert_eq!(container1.get(3), Some(&4));
	}

	#[test]
	fn test_iter_u8() {
		let data = vec![1u8, 2, 3];
		let container = NumberContainer::new(data);

		let collected: Vec<Option<u8>> = container.iter().collect();
		assert_eq!(collected, vec![Some(1), Some(2), Some(3)]);
	}

	#[test]
	fn test_slice() {
		let container = NumberContainer::from_vec(vec![10i16, 20, 30, 40]);
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(&20));
		assert_eq!(sliced.get(1), Some(&30));
	}

	#[test]
	fn test_filter() {
		use crate::util::bitvec::BitVec;
		let mut container = NumberContainer::from_vec(vec![1f32, 2.0, 3.0, 4.0]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&1.0));
		assert_eq!(container.get(1), Some(&3.0));
	}

	#[test]
	fn test_reorder() {
		let mut container = NumberContainer::from_vec(vec![10i32, 20, 30]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&30)); // was index 2
		assert_eq!(container.get(1), Some(&10)); // was index 0
		assert_eq!(container.get(2), Some(&20)); // was index 1
	}

	#[test]
	fn test_push_with_convert() {
		let mut container: NumberContainer<i32> = NumberContainer::with_capacity(3);

		// Successful conversion
		container.push_with_convert(42u32, |x| {
			if x <= i32::MAX as u32 {
				Some(x as i32)
			} else {
				None
			}
		});

		// Failed conversion
		container.push_with_convert(u32::MAX, |x| {
			if x <= i32::MAX as u32 {
				Some(x as i32)
			} else {
				None
			}
		});

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&42));
		assert_eq!(container.get(1), Some(&0)); // conversion failed, pushed default

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
	}

	#[test]
	fn test_data_access() {
		let mut container = NumberContainer::from_vec(vec![1i32, 2, 3]);

		// Test immutable access
		assert_eq!(container.data().len(), 3);

		// Test mutable access
		container.data_mut().push(4);

		assert_eq!(container.len(), 4);
		assert_eq!(container.get(3), Some(&4));
	}
}
