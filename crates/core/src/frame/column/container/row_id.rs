// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{BitVec, CowVec, RowId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowIdContainer {
    values: CowVec<RowId>,
    bitvec: BitVec,
}

impl RowIdContainer {
    pub fn new(values: Vec<RowId>, bitvec: BitVec) -> Self {
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

    pub fn from_vec(values: Vec<RowId>) -> Self {
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

    pub fn push(&mut self, value: RowId) {
        self.values.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.values.push(RowId::default());
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&RowId> {
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

    pub fn values(&self) -> &CowVec<RowId> {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut CowVec<RowId> {
        &mut self.values
    }

    pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
        self.values.extend(other.values.iter().cloned());
        self.bitvec.extend(&other.bitvec);
        Ok(())
    }

    pub fn extend_from_undefined(&mut self, len: usize) {
        self.values.extend(std::iter::repeat(RowId::default()).take(len));
        self.bitvec.extend(&BitVec::repeat(len, false));
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<RowId>> + '_ {
        self.values
            .iter()
            .zip(self.bitvec.iter())
            .map(|(&v, defined)| if defined { Some(v) } else { None })
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let new_values: Vec<RowId> = self.values.iter().skip(start).take(end - start).cloned().collect();
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
                new_values.push(RowId::default());
                new_bitvec.push(false);
            }
        }
        
        self.values = CowVec::new(new_values);
        self.bitvec = new_bitvec;
    }
}

impl Default for RowIdContainer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BitVec, RowId};

    #[test]
    fn test_new() {
        let row_id1 = RowId::new(1);
        let row_id2 = RowId::new(2);
        let row_ids = vec![row_id1, row_id2];
        let bitvec = BitVec::from_slice(&[true, true]);
        let container = RowIdContainer::new(row_ids.clone(), bitvec);
        
        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&row_ids[0]));
        assert_eq!(container.get(1), Some(&row_ids[1]));
    }

    #[test]
    fn test_from_vec() {
        let row_ids = vec![RowId::new(10), RowId::new(20), RowId::new(30)];
        let container = RowIdContainer::from_vec(row_ids.clone());
        
        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&row_ids[0]));
        assert_eq!(container.get(1), Some(&row_ids[1]));
        assert_eq!(container.get(2), Some(&row_ids[2]));
        
        // All should be defined
        for i in 0..3 {
            assert!(container.bitvec().get(i));
        }
    }

    #[test]
    fn test_with_capacity() {
        let container = RowIdContainer::with_capacity(10);
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
        assert!(container.capacity() >= 10);
    }

    #[test]
    fn test_push_with_undefined() {
        let mut container = RowIdContainer::with_capacity(3);
        let row_id1 = RowId::new(100);
        let row_id2 = RowId::new(200);
        
        container.push(row_id1);
        container.push_undefined();
        container.push(row_id2);
        
        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&row_id1));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), Some(&row_id2));
        
        assert!(container.bitvec().get(0));
        assert!(!container.bitvec().get(1));
        assert!(container.bitvec().get(2));
    }

    #[test]
    fn test_extend() {
        let row_id1 = RowId::new(1);
        let row_id2 = RowId::new(2);
        let row_id3 = RowId::new(3);
        
        let mut container1 = RowIdContainer::from_vec(vec![row_id1, row_id2]);
        let container2 = RowIdContainer::from_vec(vec![row_id3]);
        
        container1.extend(&container2).unwrap();
        
        assert_eq!(container1.len(), 3);
        assert_eq!(container1.get(0), Some(&row_id1));
        assert_eq!(container1.get(1), Some(&row_id2));
        assert_eq!(container1.get(2), Some(&row_id3));
    }

    #[test]
    fn test_extend_from_undefined() {
        let row_id = RowId::new(42);
        let mut container = RowIdContainer::from_vec(vec![row_id]);
        container.extend_from_undefined(2);
        
        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&row_id));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), None); // undefined
    }

    #[test]
    fn test_iter() {
        let row_id1 = RowId::new(10);
        let row_id2 = RowId::new(20);
        let row_id3 = RowId::new(30);
        let row_ids = vec![row_id1, row_id2, row_id3];
        let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
        let container = RowIdContainer::new(row_ids.clone(), bitvec);
        
        let collected: Vec<Option<RowId>> = container.iter().collect();
        assert_eq!(collected, vec![Some(row_ids[0]), None, Some(row_ids[2])]);
    }

    #[test]
    fn test_slice() {
        let container = RowIdContainer::from_vec(vec![
            RowId::new(1),
            RowId::new(2),
            RowId::new(3),
            RowId::new(4),
        ]);
        let sliced = container.slice(1, 3);
        
        assert_eq!(sliced.len(), 2);
        assert_eq!(sliced.get(0), Some(&RowId::new(2)));
        assert_eq!(sliced.get(1), Some(&RowId::new(3)));
    }

    #[test]
    fn test_filter() {
        let mut container = RowIdContainer::from_vec(vec![
            RowId::new(1),
            RowId::new(2),
            RowId::new(3),
            RowId::new(4),
        ]);
        let mask = BitVec::from_slice(&[true, false, true, false]);
        
        container.filter(&mask);
        
        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&RowId::new(1)));
        assert_eq!(container.get(1), Some(&RowId::new(3)));
    }

    #[test]
    fn test_reorder() {
        let mut container = RowIdContainer::from_vec(vec![
            RowId::new(10),
            RowId::new(20),
            RowId::new(30),
        ]);
        let indices = [2, 0, 1];
        
        container.reorder(&indices);
        
        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&RowId::new(30))); // was index 2
        assert_eq!(container.get(1), Some(&RowId::new(10))); // was index 0
        assert_eq!(container.get(2), Some(&RowId::new(20))); // was index 1
    }

    #[test]
    fn test_reorder_with_out_of_bounds() {
        let mut container = RowIdContainer::from_vec(vec![RowId::new(1), RowId::new(2)]);
        let indices = [1, 5, 0]; // index 5 is out of bounds
        
        container.reorder(&indices);
        
        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&RowId::new(2))); // was index 1
        assert_eq!(container.get(1), None);                 // out of bounds -> undefined
        assert_eq!(container.get(2), Some(&RowId::new(1))); // was index 0
    }

    #[test]
    fn test_default() {
        let container = RowIdContainer::default();
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
    }

    #[test]
    fn test_values_access() {
        let mut container = RowIdContainer::from_vec(vec![RowId::new(1), RowId::new(2)]);
        
        // Test immutable access
        assert_eq!(container.values().len(), 2);
        
        // Test mutable access
        container.values_mut().push(RowId::new(3));
        container.bitvec_mut().push(true);
        
        assert_eq!(container.len(), 3);
        assert_eq!(container.get(2), Some(&RowId::new(3)));
    }
}