// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::IsTemporal;
use crate::{BitVec, CowVec};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalContainer<T>
where
    T: IsTemporal,
{
    values: CowVec<T>,
    bitvec: BitVec,
}

impl<T> TemporalContainer<T>
where
    T: IsTemporal + Clone + Debug + Default,
{
    pub fn new(values: Vec<T>, bitvec: BitVec) -> Self {
        debug_assert_eq!(values.len(), bitvec.len());
        Self {
            values: CowVec::new(values),
            bitvec,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: CowVec::with_capacity(capacity),
            bitvec: BitVec::with_capacity(capacity),
        }
    }

    pub fn from_vec(values: Vec<T>) -> Self {
        let len = values.len();
        Self {
            values: CowVec::new(values),
            bitvec: BitVec::repeat(len, true),
        }
    }

    pub fn len(&self) -> usize {
        debug_assert_eq!(self.values.len(), self.bitvec.len());
        self.values.len()
    }

    pub fn capacity(&self) -> usize {
        debug_assert!(self.values.capacity() >= self.bitvec.capacity());
        self.values.capacity().min(self.bitvec.capacity())
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn push(&mut self, value: T) {
        self.values.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.values.push(T::default());
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.len() && self.bitvec.get(index) {
            self.values.get(index)
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

    pub fn values(&self) -> &CowVec<T> {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut CowVec<T> {
        &mut self.values
    }

    pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
        self.values.extend(other.values.iter().cloned());
        self.bitvec.extend(&other.bitvec);
        Ok(())
    }

    pub fn extend_from_undefined(&mut self, len: usize) {
        self.values.extend(std::iter::repeat(T::default()).take(len));
        self.bitvec.extend(&BitVec::repeat(len, false));
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_
    where
        T: Copy,
    {
        self.values
            .iter()
            .zip(self.bitvec.iter())
            .map(|(&v, defined)| if defined { Some(v) } else { None })
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let new_values: Vec<T> = self.values.iter().skip(start).take(end - start).cloned().collect();
        let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
        Self {
            values: CowVec::new(new_values),
            bitvec: BitVec::from_slice(&new_bitvec),
        }
    }

    pub fn filter(&mut self, mask: &BitVec) {
        let mut new_values = Vec::with_capacity(mask.count_ones());
        let mut new_bitvec = BitVec::with_capacity(mask.count_ones());
        
        for (i, keep) in mask.iter().enumerate() {
            if keep && i < self.len() {
                new_values.push(self.values[i].clone());
                new_bitvec.push(self.bitvec.get(i));
            }
        }
        
        self.values = CowVec::new(new_values);
        self.bitvec = new_bitvec;
    }

    pub fn reorder(&mut self, indices: &[usize]) {
        let mut new_values = Vec::with_capacity(indices.len());
        let mut new_bitvec = BitVec::with_capacity(indices.len());
        
        for &idx in indices {
            if idx < self.len() {
                new_values.push(self.values[idx].clone());
                new_bitvec.push(self.bitvec.get(idx));
            } else {
                new_values.push(T::default());
                new_bitvec.push(false);
            }
        }
        
        self.values = CowVec::new(new_values);
        self.bitvec = new_bitvec;
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
mod tests {
    use super::*;
    use crate::{BitVec, Date, DateTime, Time, Interval};

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
            assert!(container.bitvec().get(i));
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
        let intervals = vec![
            Interval::from_days(30),
            Interval::from_hours(24),
        ];
        let container = TemporalContainer::from_vec(intervals.clone());
        
        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&intervals[0]));
        assert_eq!(container.get(1), Some(&intervals[1]));
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
        
        assert!(container.bitvec().get(0));
        assert!(!container.bitvec().get(1));
        assert!(container.bitvec().get(2));
    }

    #[test]
    fn test_extend() {
        let mut container1 = TemporalContainer::from_vec(vec![
            Date::from_ymd(2023, 1, 1).unwrap(),
            Date::from_ymd(2023, 6, 15).unwrap(),
        ]);
        let container2 = TemporalContainer::from_vec(vec![
            Date::from_ymd(2023, 12, 31).unwrap(),
        ]);
        
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
        assert_eq!(container.get(1), Some(&Date::from_ymd(2023, 1, 1).unwrap()));   // was index 0
        assert_eq!(container.get(2), Some(&Date::from_ymd(2023, 6, 15).unwrap()));  // was index 1
    }

    #[test]
    fn test_default() {
        let container: TemporalContainer<Date> = TemporalContainer::default();
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
    }
}
