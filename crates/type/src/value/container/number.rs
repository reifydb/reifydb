// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	any::TypeId,
	fmt::Debug,
	mem::{forget, transmute_copy},
	ops::Deref,
};

use serde::{Deserialize, Serialize};

use crate::{
	BitVec, CowVec, Decimal, Int, IsNumber, OrderedF32, OrderedF64, Uint, Value,
	Value::{Int1, Int2, Int4, Int8, Int16, Uint1, Uint2, Uint4, Uint8, Uint16, Undefined},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumberContainer<T>
where
	T: IsNumber,
{
	data: CowVec<T>,
	bitvec: BitVec,
}

impl<T> Deref for NumberContainer<T>
where
	T: IsNumber,
{
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> NumberContainer<T>
where
	T: IsNumber + Clone + Debug + Default,
{
	pub fn new(data: Vec<T>, bitvec: BitVec) -> Self {
		debug_assert_eq!(data.len(), bitvec.len());
		Self {
			data: CowVec::new(data),
			bitvec,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
			bitvec: BitVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<T>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}

	pub fn len(&self) -> usize {
		debug_assert_eq!(self.data.len(), self.bitvec.len());
		self.data.len()
	}

	pub fn capacity(&self) -> usize {
		debug_assert!(self.data.capacity() >= self.bitvec.capacity());
		self.data.capacity().min(self.bitvec.capacity())
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn push(&mut self, value: T) {
		self.data.push(value);
		self.bitvec.push(true);
	}

	pub fn push_undefined(&mut self) {
		self.data.push(T::default());
		self.bitvec.push(false);
	}

	pub fn get(&self, index: usize) -> Option<&T> {
		if index < self.len() && self.is_defined(index) {
			self.data.get(index)
		} else {
			None
		}
	}

	pub fn bitvec(&self) -> &BitVec {
		&self.bitvec
	}

	pub fn bitvec_mut(&mut self) -> &mut BitVec {
		&mut self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && self.bitvec.get(idx)
	}

	pub fn is_fully_defined(&self) -> bool {
		self.bitvec.count_ones() == self.len()
	}

	pub fn data(&self) -> &CowVec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut CowVec<T> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].to_string()
		} else {
			"Undefined".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value
	where
		T: 'static,
	{
		if index < self.len() && self.is_defined(index) {
			let value = self.data[index].clone();

			if TypeId::of::<T>() == TypeId::of::<f32>() {
				let f_val = unsafe { transmute_copy::<T, f32>(&value) };
				OrderedF32::try_from(f_val).map(Value::Float4).unwrap_or(Undefined)
			} else if TypeId::of::<T>() == TypeId::of::<f64>() {
				let f_val = unsafe { transmute_copy::<T, f64>(&value) };
				OrderedF64::try_from(f_val).map(Value::Float8).unwrap_or(Undefined)
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
				Undefined
			}
		} else {
			Undefined
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		self.data.extend(other.data.iter().cloned());
		self.bitvec.extend(&other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		self.data.extend(std::iter::repeat(T::default()).take(len));
		self.bitvec.extend(&BitVec::repeat(len, false));
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_
	where
		T: Copy,
	{
		self.data.iter().zip(self.bitvec.iter()).map(|(&v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let new_data: Vec<T> = self.data.iter().skip(start).take(end - start).cloned().collect();
		let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
		Self {
			data: CowVec::new(new_data),
			bitvec: BitVec::from_slice(&new_bitvec),
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());
		let mut new_bitvec = BitVec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i].clone());
				new_bitvec.push(self.bitvec.get(i));
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());
		let mut new_bitvec = BitVec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data[idx].clone());
				new_bitvec.push(self.bitvec.get(idx));
			} else {
				new_data.push(T::default());
				new_bitvec.push(false);
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn push_with_convert<U>(&mut self, value: U, converter: impl FnOnce(U) -> Option<T>) {
		match converter(value) {
			Some(v) => {
				self.data.push(v);
				self.bitvec.push(true);
			}
			None => {
				self.data.push(T::default());
				self.bitvec.push(false);
			}
		}
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.take(num),
			bitvec: self.bitvec.take(num),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::BitVec;

	#[test]
	fn test_new_i32() {
		let data = vec![1, 2, 3];
		let bitvec = BitVec::from_slice(&[true, true, true]);
		let container = NumberContainer::new(data.clone(), bitvec);

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
		container.push_undefined();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&100));
		assert_eq!(container.get(1), Some(&-200));
		assert_eq!(container.get(2), None); // undefined

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(!container.is_defined(2));
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
	fn test_extend_from_undefined() {
		let mut container = NumberContainer::from_vec(vec![1i32, 2]);
		container.extend_from_undefined(2);

		assert_eq!(container.len(), 4);
		assert_eq!(container.get(0), Some(&1));
		assert_eq!(container.get(1), Some(&2));
		assert_eq!(container.get(2), None); // undefined
		assert_eq!(container.get(3), None); // undefined
	}

	#[test]
	fn test_iter_u8() {
		let data = vec![1u8, 2, 3];
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container = NumberContainer::new(data, bitvec);

		let collected: Vec<Option<u8>> = container.iter().collect();
		assert_eq!(collected, vec![Some(1), None, Some(3)]);
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
		assert_eq!(container.get(1), None); // conversion failed

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_data_access() {
		let mut container = NumberContainer::from_vec(vec![1i32, 2, 3]);

		// Test immutable access
		assert_eq!(container.data().len(), 3);

		// Test mutable access
		container.data_mut().push(4);
		container.bitvec_mut().push(true);

		assert_eq!(container.len(), 4);
		assert_eq!(container.get(3), Some(&4));
	}
}
