// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::BitVec;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoolContainer {
    values: BitVec,
    bitvec: BitVec,
}

impl BoolContainer {
    pub fn new(values: Vec<bool>, bitvec: BitVec) -> Self {
        debug_assert_eq!(values.len(), bitvec.len());
        Self { values: BitVec::from_slice(&values), bitvec }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { values: BitVec::with_capacity(capacity), bitvec: BitVec::with_capacity(capacity) }
    }

    pub fn from_vec(values: Vec<bool>) -> Self {
        let len = values.len();
        Self { values: BitVec::from_slice(&values), bitvec: BitVec::repeat(len, true) }
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
        self.values.len() == 0
    }

    pub fn push(&mut self, value: bool) {
        self.values.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.values.push(false);
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<bool> {
        if index < self.len() && self.is_defined(index) {
            Some(self.values.get(index))
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

    pub fn values(&self) -> &BitVec {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut BitVec {
        &mut self.values
    }

    pub fn as_string(&self, index: usize) -> String {
        if index < self.len() && self.is_defined(index) {
            self.values.get(index).to_string()
        } else {
            "Undefined".to_string()
        }
    }

    pub fn get_value(&self, index: usize) -> crate::Value {
        if index < self.len() && self.is_defined(index) {
            crate::Value::Bool(self.values.get(index))
        } else {
            crate::Value::Undefined
        }
    }

    pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
        self.values.extend(&other.values);
        self.bitvec.extend(&other.bitvec);
        Ok(())
    }

    pub fn extend_from_undefined(&mut self, len: usize) {
        self.values.extend(&BitVec::repeat(len, false));
        self.bitvec.extend(&BitVec::repeat(len, false));
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<bool>> + '_ {
        self.values
            .iter()
            .zip(self.bitvec.iter())
            .map(|(v, defined)| if defined { Some(v) } else { None })
    }

    pub fn into_iter(self) -> impl Iterator<Item = Option<bool>> {
        let values: Vec<bool> = self.values.iter().collect();
        let bitvec: Vec<bool> = self.bitvec.iter().collect();
        values.into_iter().zip(bitvec).map(|(v, defined)| if defined { Some(v) } else { None })
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let new_values: Vec<bool> = self.values.iter().skip(start).take(end - start).collect();
        let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
        Self { values: BitVec::from_slice(&new_values), bitvec: BitVec::from_slice(&new_bitvec) }
    }

    pub fn filter(&mut self, mask: &BitVec) {
        let mut new_values = BitVec::with_capacity(mask.count_ones());
        let mut new_bitvec = BitVec::with_capacity(mask.count_ones());

        for (i, keep) in mask.iter().enumerate() {
            if keep && i < self.len() {
                new_values.push(self.values.get(i));
                new_bitvec.push(self.bitvec.get(i));
            }
        }

        self.values = new_values;
        self.bitvec = new_bitvec;
    }

    pub fn reorder(&mut self, indices: &[usize]) {
        let mut new_values = BitVec::with_capacity(indices.len());
        let mut new_bitvec = BitVec::with_capacity(indices.len());

        for &idx in indices {
            if idx < self.len() {
                new_values.push(self.values.get(idx));
                new_bitvec.push(self.bitvec.get(idx));
            } else {
                new_values.push(false);
                new_bitvec.push(false);
            }
        }

        self.values = new_values;
        self.bitvec = new_bitvec;
    }

    pub fn take(&self, num: usize) -> Self {
        Self { values: self.values.take(num), bitvec: self.bitvec.take(num) }
    }
}

impl Deref for BoolContainer {
    type Target = BitVec;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl Default for BoolContainer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BitVec;

    #[test]
    fn test_new() {
        let values = vec![true, false, true];
        let bitvec = BitVec::from_slice(&[true, true, true]);
        let container = BoolContainer::new(values.clone(), bitvec);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(true));
        assert_eq!(container.get(1), Some(false));
        assert_eq!(container.get(2), Some(true));
    }

    #[test]
    fn test_from_vec() {
        let values = vec![true, false, true];
        let container = BoolContainer::from_vec(values);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(true));
        assert_eq!(container.get(1), Some(false));
        assert_eq!(container.get(2), Some(true));

        // All should be defined
        for i in 0..3 {
            assert!(container.is_defined(i));
        }
    }

    #[test]
    fn test_with_capacity() {
        let container = BoolContainer::with_capacity(10);
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
        assert!(container.capacity() >= 10);
    }

    #[test]
    fn test_push() {
        let mut container = BoolContainer::with_capacity(3);

        container.push(true);
        container.push(false);
        container.push_undefined();

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(true));
        assert_eq!(container.get(1), Some(false));
        assert_eq!(container.get(2), None); // undefined

        assert!(container.is_defined(0));
        assert!(container.is_defined(1));
        assert!(!container.is_defined(2));
    }

    #[test]
    fn test_extend() {
        let mut container1 = BoolContainer::from_vec(vec![true, false]);
        let container2 = BoolContainer::from_vec(vec![false, true]);

        container1.extend(&container2).unwrap();

        assert_eq!(container1.len(), 4);
        assert_eq!(container1.get(0), Some(true));
        assert_eq!(container1.get(1), Some(false));
        assert_eq!(container1.get(2), Some(false));
        assert_eq!(container1.get(3), Some(true));
    }

    #[test]
    fn test_iter() {
        let values = vec![true, false, true];
        let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
        let container = BoolContainer::new(values, bitvec);

        let collected: Vec<Option<bool>> = container.iter().collect();
        assert_eq!(collected, vec![Some(true), None, Some(true)]);
    }

    #[test]
    fn test_slice() {
        let container = BoolContainer::from_vec(vec![true, false, true, false]);
        let sliced = container.slice(1, 3);

        assert_eq!(sliced.len(), 2);
        assert_eq!(sliced.get(0), Some(false));
        assert_eq!(sliced.get(1), Some(true));
    }

    #[test]
    fn test_filter() {
        let mut container = BoolContainer::from_vec(vec![true, false, true, false]);
        let mask = BitVec::from_slice(&[true, false, true, false]);

        container.filter(&mask);

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(true));
        assert_eq!(container.get(1), Some(true));
    }

    #[test]
    fn test_reorder() {
        let mut container = BoolContainer::from_vec(vec![true, false, true]);
        let indices = [2, 0, 1];

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(true)); // was index 2
        assert_eq!(container.get(1), Some(true)); // was index 0
        assert_eq!(container.get(2), Some(false)); // was index 1
    }

    #[test]
    fn test_reorder_with_out_of_bounds() {
        let mut container = BoolContainer::from_vec(vec![true, false]);
        let indices = [1, 5, 0]; // index 5 is out of bounds

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(false)); // was index 1
        assert_eq!(container.get(1), None); // out of bounds -> undefined
        assert_eq!(container.get(2), Some(true)); // was index 0
    }

    #[test]
    fn test_default() {
        let container = BoolContainer::default();
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
    }
}
