// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	any::TypeId,
	fmt::{self, Debug},
	mem::transmute_copy,
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{Value, date::Date, datetime::DateTime, duration::Duration, is::IsTemporal, time::Time},
};

pub struct TemporalContainer<T, S: Storage = Cow>
where
	T: IsTemporal,
{
	data: S::Vec<T>,
	bitvec: S::BitVec,
}

impl<T: IsTemporal, S: Storage> Clone for TemporalContainer<T, S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<T: IsTemporal + Debug, S: Storage> Debug for TemporalContainer<T, S>
where
	S::Vec<T>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TemporalContainer").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<T: IsTemporal, S: Storage> PartialEq for TemporalContainer<T, S>
where
	S::Vec<T>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl<T: IsTemporal + Serialize> Serialize for TemporalContainer<T, Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a, T: Clone + PartialEq + Serialize> {
			data: &'a CowVec<T>,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de, T: IsTemporal + Deserialize<'de>> Deserialize<'de> for TemporalContainer<T, Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: CowVec<T>,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(TemporalContainer {
			data: h.data,
			bitvec: h.bitvec,
		})
	}
}

impl<T: IsTemporal, S: Storage> Deref for TemporalContainer<T, S> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> TemporalContainer<T, Cow>
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
}

impl<T, S: Storage> TemporalContainer<T, S>
where
	T: IsTemporal + Clone + Debug + Default,
{
	pub fn from_parts(data: S::Vec<T>, bitvec: S::BitVec) -> Self {
		Self {
			data,
			bitvec,
		}
	}

	pub fn len(&self) -> usize {
		debug_assert_eq!(DataVec::len(&self.data), DataBitVec::len(&self.bitvec));
		DataVec::len(&self.data)
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data).min(DataBitVec::capacity(&self.bitvec))
	}

	pub fn is_empty(&self) -> bool {
		DataVec::is_empty(&self.data)
	}

	pub fn clear(&mut self) {
		DataVec::clear(&mut self.data);
		DataBitVec::clear(&mut self.bitvec);
	}

	pub fn push(&mut self, value: T) {
		DataVec::push(&mut self.data, value);
		DataBitVec::push(&mut self.bitvec, true);
	}

	pub fn push_undefined(&mut self) {
		DataVec::push(&mut self.data, T::default());
		DataBitVec::push(&mut self.bitvec, false);
	}

	pub fn get(&self, index: usize) -> Option<&T> {
		if index < self.len() && self.is_defined(index) {
			DataVec::get(&self.data, index)
		} else {
			None
		}
	}

	pub fn bitvec(&self) -> &S::BitVec {
		&self.bitvec
	}

	pub fn bitvec_mut(&mut self) -> &mut S::BitVec {
		&mut self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && DataBitVec::get(&self.bitvec, idx)
	}

	pub fn is_fully_defined(&self) -> bool {
		DataBitVec::count_ones(&self.bitvec) == self.len()
	}

	pub fn data(&self) -> &S::Vec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<T> {
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
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		DataBitVec::extend_from(&mut self.bitvec, &other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		for _ in 0..len {
			DataVec::push(&mut self.data, T::default());
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_
	where
		T: Copy,
	{
		self.data.iter().zip(DataBitVec::iter(&self.bitvec)).map(|(&v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataVec::spawn(&self.data, count);
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, count);
		for i in start..(start + count) {
			DataVec::push(&mut new_data, self.data[i].clone());
			DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
		}
		Self {
			data: new_data,
			bitvec: new_bitvec,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask));
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataVec::push(&mut new_data, self.data[i].clone());
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataVec::spawn(&self.data, indices.len());
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, indices.len());

		for &idx in indices {
			if idx < self.len() {
				DataVec::push(&mut new_data, self.data[idx].clone());
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, idx));
			} else {
				DataVec::push(&mut new_data, T::default());
				DataBitVec::push(&mut new_bitvec, false);
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
			bitvec: DataBitVec::take(&self.bitvec, num),
		}
	}
}

impl<T> Default for TemporalContainer<T, Cow>
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
