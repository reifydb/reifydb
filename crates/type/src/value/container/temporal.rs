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
	util::cowvec::CowVec,
	value::{Value, date::Date, datetime::DateTime, duration::Duration, is::IsTemporal, time::Time},
};

pub struct TemporalContainer<T, S: Storage = Cow>
where
	T: IsTemporal,
{
	data: S::Vec<T>,
}

impl<T: IsTemporal, S: Storage> Clone for TemporalContainer<T, S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<T: IsTemporal + Debug, S: Storage> Debug for TemporalContainer<T, S>
where
	S::Vec<T>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TemporalContainer").field("data", &self.data).finish()
	}
}

impl<T: IsTemporal, S: Storage> PartialEq for TemporalContainer<T, S>
where
	S::Vec<T>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl<T: IsTemporal + Serialize> Serialize for TemporalContainer<T, Cow> {
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

impl<'de, T: IsTemporal + Deserialize<'de>> Deserialize<'de> for TemporalContainer<T, Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: CowVec<T>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(TemporalContainer {
			data: h.data,
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

impl<T, S: Storage> TemporalContainer<T, S>
where
	T: IsTemporal + Clone + Debug + Default,
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
				Value::None
			}
		} else {
			Value::None
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

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
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
	fn test_push_with_default() {
		let mut container: TemporalContainer<Date> = TemporalContainer::with_capacity(3);

		container.push(Date::from_ymd(2023, 1, 1).unwrap());
		container.push_default();
		container.push(Date::from_ymd(2023, 12, 31).unwrap());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&Date::from_ymd(2023, 1, 1).unwrap()));
		assert_eq!(container.get(1), Some(&Date::default())); // push_default pushes default
		assert_eq!(container.get(2), Some(&Date::from_ymd(2023, 12, 31).unwrap()));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
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
	}

	#[test]
	fn test_iter() {
		let dates = vec![
			Date::from_ymd(2023, 1, 1).unwrap(),
			Date::from_ymd(2023, 6, 15).unwrap(),
			Date::from_ymd(2023, 12, 31).unwrap(),
		];
		let container = TemporalContainer::new(dates.clone());

		let collected: Vec<Option<Date>> = container.iter().collect();
		assert_eq!(collected, vec![Some(dates[0]), Some(dates[1]), Some(dates[2])]);
	}

	#[test]
	fn test_default() {
		let container: TemporalContainer<Date> = TemporalContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
