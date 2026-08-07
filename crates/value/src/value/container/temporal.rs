// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	util::bitvec::BitVec,
	value::{Value, is::IsTemporal},
};

pub struct TemporalContainer<T>
where
	T: IsTemporal,
{
	data: Vec<T>,
}

impl<T: IsTemporal> Clone for TemporalContainer<T> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<T: IsTemporal + Debug> Debug for TemporalContainer<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TemporalContainer").field("data", &self.data).finish()
	}
}

impl<T: IsTemporal> PartialEq for TemporalContainer<T> {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl<T: IsTemporal + Serialize> Serialize for TemporalContainer<T> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a, T: Clone + PartialEq + Serialize> {
			data: &'a Vec<T>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de, T: IsTemporal + Deserialize<'de>> Deserialize<'de> for TemporalContainer<T> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: Vec<T>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(TemporalContainer {
			data: h.data,
		})
	}
}

impl<T: IsTemporal> Deref for TemporalContainer<T> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> TemporalContainer<T>
where
	T: IsTemporal + Clone + Debug + Default,
{
	pub fn new(data: Vec<T>) -> Self {
		Self {
			data,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<T>) -> Self {
		Self {
			data,
		}
	}
}

impl<T> TemporalContainer<T>
where
	T: IsTemporal + Clone + Debug + Default,
{
	pub fn from_parts(data: Vec<T>) -> Self {
		Self {
			data,
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn heap_size(&self) -> usize {
		self.capacity() * size_of::<T>()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, value: T) {
		self.data.push(value);
	}

	pub fn push_default(&mut self) {
		self.data.push(T::default());
	}

	pub fn get(&self, index: usize) -> Option<&T> {
		if index < self.len() {
			self.data.get(index)
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

	pub fn data(&self) -> &Vec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut Vec<T> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			self.data[index].to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() {
			self.data[index].to_value()
		} else {
			Value::none()
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend(other.data.iter().cloned());
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
		let mut new_data = Vec::with_capacity(count);
		for i in start..(start + count) {
			new_data.push(self.data[i].clone());
		}
		Self {
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i].clone());
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data[idx].clone());
			} else {
				new_data.push(T::default());
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data[..num.min(self.data.len())].to_vec(),
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
	use crate::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

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
			DateTime::from_epoch_secs(1000000000).unwrap(),
			DateTime::from_epoch_secs(2000000000).unwrap(),
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
		let durations = vec![Duration::from_days(30).unwrap(), Duration::from_hours(24).unwrap()];
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
	fn testault() {
		let container: TemporalContainer<Date> = TemporalContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
