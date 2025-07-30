// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{BitVec, CowVec, Value};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StringContainer {
    values: CowVec<String>,
    bitvec: BitVec,
}

impl StringContainer {
    pub fn new(values: Vec<String>, bitvec: BitVec) -> Self {
        debug_assert_eq!(values.len(), bitvec.len());
        Self { values: CowVec::new(values), bitvec }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { values: CowVec::with_capacity(capacity), bitvec: BitVec::with_capacity(capacity) }
    }

    pub fn from_vec(values: Vec<String>) -> Self {
        let len = values.len();
        Self { values: CowVec::new(values), bitvec: BitVec::repeat(len, true) }
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

    pub fn push(&mut self, value: String) {
        self.values.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.values.push(String::new());
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&String> {
        if index < self.len() && self.is_defined(index) { self.values.get(index) } else { None }
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

    pub fn values(&self) -> &CowVec<String> {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut CowVec<String> {
        &mut self.values
    }

    pub fn as_string(&self, index: usize) -> String {
        if index < self.len() && self.is_defined(index) {
            self.values[index].clone()
        } else {
            "Undefined".to_string()
        }
    }

    pub fn get_value(&self, index: usize) -> Value {
        if index < self.len() && self.is_defined(index) {
            Value::Utf8(self.values[index].clone())
        } else {
            Value::Undefined
        }
    }

    pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
        self.values.extend(other.values.iter().cloned());
        self.bitvec.extend(&other.bitvec);
        Ok(())
    }

    pub fn extend_from_undefined(&mut self, len: usize) {
        self.values.extend(std::iter::repeat(String::new()).take(len));
        self.bitvec.extend(&BitVec::repeat(len, false));
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&String>> + '_ {
        self.values
            .iter()
            .zip(self.bitvec.iter())
            .map(|(v, defined)| if defined { Some(v) } else { None })
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let new_values: Vec<String> =
            self.values.iter().skip(start).take(end - start).cloned().collect();
        let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
        Self { values: CowVec::new(new_values), bitvec: BitVec::from_slice(&new_bitvec) }
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
                new_values.push(String::new());
                new_bitvec.push(false);
            }
        }

        self.values = CowVec::new(new_values);
        self.bitvec = new_bitvec;
    }

    pub fn take(&self, num: usize) -> Self {
        Self {
            values: self.values.take(num),
            bitvec: self.bitvec.take(num),
        }
    }
}

impl Deref for StringContainer {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        self.values.as_slice()
    }
}

impl Default for StringContainer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::BitVec;

    #[test]
    fn test_new() {
        let values = vec!["hello".to_string(), "world".to_string(), "test".to_string()];
        let bitvec = BitVec::from_slice(&[true, true, true]);
        let container = StringContainer::new(values.clone(), bitvec);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&"hello".to_string()));
        assert_eq!(container.get(1), Some(&"world".to_string()));
        assert_eq!(container.get(2), Some(&"test".to_string()));
    }

    #[test]
    fn test_from_vec() {
        let values = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let container = StringContainer::from_vec(values);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&"foo".to_string()));
        assert_eq!(container.get(1), Some(&"bar".to_string()));
        assert_eq!(container.get(2), Some(&"baz".to_string()));

        // All should be defined
        for i in 0..3 {
            assert!(container.is_defined(i));
        }
    }

    #[test]
    fn test_with_capacity() {
        let container = StringContainer::with_capacity(10);
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
        assert!(container.capacity() >= 10);
    }

    #[test]
    fn test_push() {
        let mut container = StringContainer::with_capacity(3);

        container.push("first".to_string());
        container.push("second".to_string());
        container.push_undefined();

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&"first".to_string()));
        assert_eq!(container.get(1), Some(&"second".to_string()));
        assert_eq!(container.get(2), None); // undefined

        assert!(container.is_defined(0));
        assert!(container.is_defined(1));
        assert!(!container.is_defined(2));
    }

    #[test]
    fn test_extend() {
        let mut container1 = StringContainer::from_vec(vec!["a".to_string(), "b".to_string()]);
        let container2 = StringContainer::from_vec(vec!["c".to_string(), "d".to_string()]);

        container1.extend(&container2).unwrap();

        assert_eq!(container1.len(), 4);
        assert_eq!(container1.get(0), Some(&"a".to_string()));
        assert_eq!(container1.get(1), Some(&"b".to_string()));
        assert_eq!(container1.get(2), Some(&"c".to_string()));
        assert_eq!(container1.get(3), Some(&"d".to_string()));
    }

    #[test]
    fn test_extend_from_undefined() {
        let mut container = StringContainer::from_vec(vec!["test".to_string()]);
        container.extend_from_undefined(2);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&"test".to_string()));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), None); // undefined
    }

    #[test]
    fn test_iter() {
        let values = vec!["x".to_string(), "y".to_string(), "z".to_string()];
        let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
        let container = StringContainer::new(values, bitvec);

        let collected: Vec<Option<&String>> = container.iter().collect();
        assert_eq!(collected, vec![Some(&"x".to_string()), None, Some(&"z".to_string())]);
    }

    #[test]
    fn test_slice() {
        let container = StringContainer::from_vec(vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
            "four".to_string(),
        ]);
        let sliced = container.slice(1, 3);

        assert_eq!(sliced.len(), 2);
        assert_eq!(sliced.get(0), Some(&"two".to_string()));
        assert_eq!(sliced.get(1), Some(&"three".to_string()));
    }

    #[test]
    fn test_filter() {
        let mut container = StringContainer::from_vec(vec![
            "keep".to_string(),
            "drop".to_string(),
            "keep".to_string(),
            "drop".to_string(),
        ]);
        let mask = BitVec::from_slice(&[true, false, true, false]);

        container.filter(&mask);

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&"keep".to_string()));
        assert_eq!(container.get(1), Some(&"keep".to_string()));
    }

    #[test]
    fn test_reorder() {
        let mut container = StringContainer::from_vec(vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ]);
        let indices = [2, 0, 1];

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&"third".to_string())); // was index 2
        assert_eq!(container.get(1), Some(&"first".to_string())); // was index 0
        assert_eq!(container.get(2), Some(&"second".to_string())); // was index 1
    }

    #[test]
    fn test_reorder_with_out_of_bounds() {
        let mut container = StringContainer::from_vec(vec!["a".to_string(), "b".to_string()]);
        let indices = [1, 5, 0]; // index 5 is out of bounds

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&"b".to_string())); // was index 1
        assert_eq!(container.get(1), None); // out of bounds -> undefined
        assert_eq!(container.get(2), Some(&"a".to_string())); // was index 0
    }

    #[test]
    fn test_empty_strings() {
        let mut container = StringContainer::with_capacity(2);
        container.push("".to_string()); // empty string
        container.push_undefined();

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&"".to_string()));
        assert_eq!(container.get(1), None);

        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
    }

    #[test]
    fn test_default() {
        let container = StringContainer::default();
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
    }
}
