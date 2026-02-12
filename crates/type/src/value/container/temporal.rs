// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{any::TypeId, fmt::Debug, mem::transmute_copy, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::{
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{Value, date::Date, datetime::DateTime, duration::Duration, is::IsTemporal, time::Time},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalContainer<T>
where
	T: IsTemporal,
{
	data: CowVec<T>,
	bitvec: BitVec,
}

impl<T> Deref for TemporalContainer<T>
where
	T: IsTemporal,
{
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> TemporalContainer<T>
where
	T: IsTemporal + Clone + Debug + Default,
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

	pub fn clear(&mut self) {
		self.data.clear();
		self.bitvec.clear();
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
			let value = &self.data[index];

			if TypeId::of::<T>() == TypeId::of::<Date>() {
				let date_val = unsafe { transmute_copy::<T, Date>(value) };
				Value::Date(date_val)
			} else if TypeId::of::<T>() == TypeId::of::<DateTime>() {
				let datetime_val = unsafe { transmute_copy::<T, DateTime>(value) };
				Value::DateTime(datetime_val)
			} else if TypeId::of::<T>() == TypeId::of::<Time>() {
				let time_val = unsafe { transmute_copy::<T, Time>(value) };
				Value::Time(time_val)
			} else if TypeId::of::<T>() == TypeId::of::<Duration>() {
				let duration_val = unsafe { transmute_copy::<T, Duration>(value) };
				Value::Duration(duration_val)
			} else {
				Value::Undefined
			}
		} else {
			Value::Undefined
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

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.take(num),
			bitvec: self.bitvec.take(num),
		}
	}
}

impl<T> Default for TemporalContainer<T>
where
	T: IsTemporal + Clone + Debug + Default,
{
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_date_container() {
		let dates = vec![
			Date::from_ymd(2023, 1, 1).unwrap(),
			Date::from_ymd(2023, 6, 15).unwrap(),
			Date::from_ymd(2023, 12, 31).unwrap(),
		];
		let container = TemporalContainer::from_vec(dates.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&dates[0]));
		assert_eq!(container.get(1), Some(&dates[1]));
		assert_eq!(container.get(2), Some(&dates[2]));

		// All should be defined
		for i in 0..3 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_datetime_container() {
		let datetimes = vec![
			DateTime::from_timestamp(1000000000).unwrap(),
			DateTime::from_timestamp(2000000000).unwrap(),
		];
		let container = TemporalContainer::from_vec(datetimes.clone());

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&datetimes[0]));
		assert_eq!(container.get(1), Some(&datetimes[1]));
	}

	#[test]
	fn test_time_container() {
		let times = vec![
			Time::from_hms(9, 0, 0).unwrap(),
			Time::from_hms(12, 30, 45).unwrap(),
			Time::from_hms(23, 59, 59).unwrap(),
		];
		let container = TemporalContainer::from_vec(times.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&times[0]));
		assert_eq!(container.get(1), Some(&times[1]));
		assert_eq!(container.get(2), Some(&times[2]));
	}

	#[test]
	fn test_interval_container() {
		let durations = vec![Duration::from_days(30), Duration::from_hours(24)];
		let container = TemporalContainer::from_vec(durations.clone());

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&durations[0]));
		assert_eq!(container.get(1), Some(&durations[1]));
	}

	#[test]
	fn test_with_capacity() {
		let container: TemporalContainer<Date> = TemporalContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_with_undefined() {
		let mut container: TemporalContainer<Date> = TemporalContainer::with_capacity(3);

		container.push(Date::from_ymd(2023, 1, 1).unwrap());
		container.push_undefined();
		container.push(Date::from_ymd(2023, 12, 31).unwrap());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&Date::from_ymd(2023, 1, 1).unwrap()));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), Some(&Date::from_ymd(2023, 12, 31).unwrap()));

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let mut container1 = TemporalContainer::from_vec(vec![
			Date::from_ymd(2023, 1, 1).unwrap(),
			Date::from_ymd(2023, 6, 15).unwrap(),
		]);
		let container2 = TemporalContainer::from_vec(vec![Date::from_ymd(2023, 12, 31).unwrap()]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 3);
		assert_eq!(container1.get(0), Some(&Date::from_ymd(2023, 1, 1).unwrap()));
		assert_eq!(container1.get(1), Some(&Date::from_ymd(2023, 6, 15).unwrap()));
		assert_eq!(container1.get(2), Some(&Date::from_ymd(2023, 12, 31).unwrap()));
	}

	#[test]
	fn test_extend_from_undefined() {
		let mut container = TemporalContainer::from_vec(vec![Date::from_ymd(2023, 1, 1).unwrap()]);
		container.extend_from_undefined(2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&Date::from_ymd(2023, 1, 1).unwrap()));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), None); // undefined
	}

	#[test]
	fn test_iter() {
		let dates = vec![
			Date::from_ymd(2023, 1, 1).unwrap(),
			Date::from_ymd(2023, 6, 15).unwrap(),
			Date::from_ymd(2023, 12, 31).unwrap(),
		];
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container = TemporalContainer::new(dates.clone(), bitvec);

		let collected: Vec<Option<Date>> = container.iter().collect();
		assert_eq!(collected, vec![Some(dates[0]), None, Some(dates[2])]);
	}

	#[test]
	fn test_slice() {
		let container = TemporalContainer::from_vec(vec![
			Time::from_hms(9, 0, 0).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
			Time::from_hms(15, 0, 0).unwrap(),
			Time::from_hms(18, 0, 0).unwrap(),
		]);
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(&Time::from_hms(12, 0, 0).unwrap()));
		assert_eq!(sliced.get(1), Some(&Time::from_hms(15, 0, 0).unwrap()));
	}

	#[test]
	fn test_filter() {
		let mut container = TemporalContainer::from_vec(vec![
			Date::from_ymd(2023, 1, 1).unwrap(),
			Date::from_ymd(2023, 2, 1).unwrap(),
			Date::from_ymd(2023, 3, 1).unwrap(),
			Date::from_ymd(2023, 4, 1).unwrap(),
		]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&Date::from_ymd(2023, 1, 1).unwrap()));
		assert_eq!(container.get(1), Some(&Date::from_ymd(2023, 3, 1).unwrap()));
	}

	#[test]
	fn test_reorder() {
		let mut container = TemporalContainer::from_vec(vec![
			Date::from_ymd(2023, 1, 1).unwrap(),
			Date::from_ymd(2023, 6, 15).unwrap(),
			Date::from_ymd(2023, 12, 31).unwrap(),
		]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&Date::from_ymd(2023, 12, 31).unwrap())); // was index 2
		assert_eq!(container.get(1), Some(&Date::from_ymd(2023, 1, 1).unwrap())); // was index 0
		assert_eq!(container.get(2), Some(&Date::from_ymd(2023, 6, 15).unwrap())); // was index 1
	}

	#[test]
	fn test_default() {
		let container: TemporalContainer<Date> = TemporalContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
