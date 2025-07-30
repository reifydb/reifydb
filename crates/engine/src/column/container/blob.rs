// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::Blob;
use reifydb_core::{BitVec, CowVec, Value};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlobContainer {
    data: CowVec<Blob>,
    bitvec: BitVec,
}

impl Deref for BlobContainer {
    type Target = [Blob];

    fn deref(&self) -> &Self::Target {
        self.data.as_slice()
    }
}

impl BlobContainer {
    pub fn new(data: Vec<Blob>, bitvec: BitVec) -> Self {
        debug_assert_eq!(data.len(), bitvec.len());
        Self { data: CowVec::new(data), bitvec }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { data: CowVec::with_capacity(capacity), bitvec: BitVec::with_capacity(capacity) }
    }

    pub fn from_vec(data: Vec<Blob>) -> Self {
        let len = data.len();
        Self { data: CowVec::new(data), bitvec: BitVec::repeat(len, true) }
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

    pub fn push(&mut self, value: Blob) {
        self.data.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.data.push(Blob::new(vec![]));
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&Blob> {
        if index < self.len() && self.is_defined(index) { self.data.get(index) } else { None }
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

    pub fn data(&self) -> &CowVec<Blob> {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut CowVec<Blob> {
        &mut self.data
    }

    pub fn as_string(&self, index: usize) -> String {
        if index < self.len() && self.is_defined(index) {
            self.data[index].to_string()
        } else {
            "Undefined".to_string()
        }
    }

    pub fn get_value(&self, index: usize) -> Value {
        if index < self.len() && self.is_defined(index) {
            Value::Blob(self.data[index].clone())
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
        self.data.extend(std::iter::repeat(Blob::new(vec![])).take(len));
        self.bitvec.extend(&BitVec::repeat(len, false));
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&Blob>> + '_ {
        self.data
            .iter()
            .zip(self.bitvec.iter())
            .map(|(v, defined)| if defined { Some(v) } else { None })
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let new_data: Vec<Blob> =
            self.data.iter().skip(start).take(end - start).cloned().collect();
        let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
        Self { data: CowVec::new(new_data), bitvec: BitVec::from_slice(&new_bitvec) }
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
                new_data.push(Blob::new(vec![]));
                new_bitvec.push(false);
            }
        }

        self.data = CowVec::new(new_data);
        self.bitvec = new_bitvec;
    }

    pub fn take(&self, num: usize) -> Self {
        Self { data: self.data.take(num), bitvec: self.bitvec.take(num) }
    }
}

impl Default for BlobContainer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::BitVec;
    use reifydb_core::value::Blob;

    #[test]
    fn test_new() {
        let blob1 = Blob::new(vec![1, 2, 3]);
        let blob2 = Blob::new(vec![4, 5, 6]);
        let blobs = vec![blob1.clone(), blob2.clone()];
        let bitvec = BitVec::from_slice(&[true, true]);
        let container = BlobContainer::new(blobs, bitvec);

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&blob1));
        assert_eq!(container.get(1), Some(&blob2));
    }

    #[test]
    fn test_from_vec() {
        let blob1 = Blob::new(vec![10, 20, 30]);
        let blob2 = Blob::new(vec![40, 50]);
        let blobs = vec![blob1.clone(), blob2.clone()];
        let container = BlobContainer::from_vec(blobs);

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&blob1));
        assert_eq!(container.get(1), Some(&blob2));

        // All should be defined
        for i in 0..2 {
            assert!(container.is_defined(i));
        }
    }

    #[test]
    fn test_with_capacity() {
        let container = BlobContainer::with_capacity(10);
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
        assert!(container.capacity() >= 10);
    }

    #[test]
    fn test_push_with_undefined() {
        let mut container = BlobContainer::with_capacity(3);
        let blob1 = Blob::new(vec![1, 2, 3]);
        let blob2 = Blob::new(vec![7, 8, 9]);

        container.push(blob1.clone());
        container.push_undefined();
        container.push(blob2.clone());

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&blob1));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), Some(&blob2));

        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
        assert!(container.is_defined(2));
    }

    #[test]
    fn test_extend() {
        let blob1 = Blob::new(vec![1, 2]);
        let blob2 = Blob::new(vec![3, 4]);
        let blob3 = Blob::new(vec![5, 6]);

        let mut container1 = BlobContainer::from_vec(vec![blob1.clone(), blob2.clone()]);
        let container2 = BlobContainer::from_vec(vec![blob3.clone()]);

        container1.extend(&container2).unwrap();

        assert_eq!(container1.len(), 3);
        assert_eq!(container1.get(0), Some(&blob1));
        assert_eq!(container1.get(1), Some(&blob2));
        assert_eq!(container1.get(2), Some(&blob3));
    }

    #[test]
    fn test_extend_from_undefined() {
        let blob = Blob::new(vec![100, 200]);
        let mut container = BlobContainer::from_vec(vec![blob.clone()]);
        container.extend_from_undefined(2);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&blob));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), None); // undefined
    }

    #[test]
    fn test_iter() {
        let blob1 = Blob::new(vec![1]);
        let blob2 = Blob::new(vec![2]);
        let blob3 = Blob::new(vec![3]);
        let blobs = vec![blob1.clone(), blob2, blob3.clone()];
        let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
        let container = BlobContainer::new(blobs, bitvec);

        let collected: Vec<Option<&Blob>> = container.iter().collect();
        assert_eq!(collected, vec![Some(&blob1), None, Some(&blob3)]);
    }

    #[test]
    fn test_slice() {
        let blobs =
            vec![Blob::new(vec![1]), Blob::new(vec![2]), Blob::new(vec![3]), Blob::new(vec![4])];
        let container = BlobContainer::from_vec(blobs.clone());
        let sliced = container.slice(1, 3);

        assert_eq!(sliced.len(), 2);
        assert_eq!(sliced.get(0), Some(&blobs[1]));
        assert_eq!(sliced.get(1), Some(&blobs[2]));
    }

    #[test]
    fn test_filter() {
        let blobs =
            vec![Blob::new(vec![1]), Blob::new(vec![2]), Blob::new(vec![3]), Blob::new(vec![4])];
        let mut container = BlobContainer::from_vec(blobs.clone());
        let mask = BitVec::from_slice(&[true, false, true, false]);

        container.filter(&mask);

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&blobs[0]));
        assert_eq!(container.get(1), Some(&blobs[2]));
    }

    #[test]
    fn test_reorder() {
        let blobs = vec![Blob::new(vec![10]), Blob::new(vec![20]), Blob::new(vec![30])];
        let mut container = BlobContainer::from_vec(blobs.clone());
        let indices = [2, 0, 1];

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&blobs[2])); // was index 2
        assert_eq!(container.get(1), Some(&blobs[0])); // was index 0
        assert_eq!(container.get(2), Some(&blobs[1])); // was index 1
    }

    #[test]
    fn test_reorder_with_out_of_bounds() {
        let blobs = vec![Blob::new(vec![1]), Blob::new(vec![2])];
        let mut container = BlobContainer::from_vec(blobs.clone());
        let indices = [1, 5, 0]; // index 5 is out of bounds

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&blobs[1])); // was index 1
        assert_eq!(container.get(1), None); // out of bounds -> undefined
        assert_eq!(container.get(2), Some(&blobs[0])); // was index 0
    }

    #[test]
    fn test_empty_blobs() {
        let mut container = BlobContainer::with_capacity(2);
        let empty_blob = Blob::new(vec![]);
        let data_blob = Blob::new(vec![1, 2, 3]);

        container.push(empty_blob.clone());
        container.push(data_blob.clone());

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&empty_blob));
        assert_eq!(container.get(1), Some(&data_blob));

        assert!(container.is_defined(0));
        assert!(container.is_defined(1));
    }

    #[test]
    fn test_default() {
        let container = BlobContainer::default();
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
    }
}
