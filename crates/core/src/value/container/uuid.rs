// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::IsUuid;
use crate::value::uuid::{Uuid4, Uuid7};
use crate::{BitVec, CowVec, Value};
use serde::{Deserialize, Serialize};
use std::any::TypeId;
use std::fmt::Debug;
use std::mem::transmute_copy;
use std::ops::Deref;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UuidContainer<T>
where
    T: IsUuid,
{
    data: CowVec<T>,
    bitvec: BitVec,
}

impl<T> Deref for UuidContainer<T>
where
    T: IsUuid,
{
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data.as_slice()
    }
}

impl<T> UuidContainer<T>
where
    T: IsUuid + Clone + Debug + Default,
{
    pub fn new(data: Vec<T>, bitvec: BitVec) -> Self {
        debug_assert_eq!(data.len(), bitvec.len());
        Self { data: CowVec::new(data), bitvec }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { data: CowVec::with_capacity(capacity), bitvec: BitVec::with_capacity(capacity) }
    }

    pub fn from_vec(data: Vec<T>) -> Self {
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

    pub fn push(&mut self, value: T) {
        self.data.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.data.push(T::default());
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
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
            let value = self.data[index];

            if TypeId::of::<T>() == TypeId::of::<Uuid4>() {
                let uuid_val = unsafe { transmute_copy::<T, Uuid4>(&value) };
                Value::Uuid4(uuid_val)
            } else if TypeId::of::<T>() == TypeId::of::<Uuid7>() {
                let uuid_val = unsafe { transmute_copy::<T, Uuid7>(&value) };
                Value::Uuid7(uuid_val)
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
        self.data
            .iter()
            .zip(self.bitvec.iter())
            .map(|(&v, defined)| if defined { Some(v) } else { None })
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let new_data: Vec<T> = self.data.iter().skip(start).take(end - start).cloned().collect();
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
                new_data.push(T::default());
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

impl<T> Default for UuidContainer<T>
where
    T: IsUuid + Clone + Debug + Default,
{
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BitVec;
    use crate::value::uuid::{Uuid4, Uuid7};

    #[test]
    fn test_uuid4_container() {
        let uuid1 = Uuid4::generate();
        let uuid2 = Uuid4::generate();
        let uuids = vec![uuid1, uuid2];
        let container = UuidContainer::from_vec(uuids.clone());

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&uuids[0]));
        assert_eq!(container.get(1), Some(&uuids[1]));

        // All should be defined
        for i in 0..2 {
            assert!(container.is_defined(i));
        }
    }

    #[test]
    fn test_uuid7_container() {
        let uuid1 = Uuid7::generate();
        let uuid2 = Uuid7::generate();
        let uuids = vec![uuid1, uuid2];
        let container = UuidContainer::from_vec(uuids.clone());

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&uuids[0]));
        assert_eq!(container.get(1), Some(&uuids[1]));
    }

    #[test]
    fn test_with_capacity() {
        let container: UuidContainer<Uuid4> = UuidContainer::with_capacity(10);
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
        assert!(container.capacity() >= 10);
    }

    #[test]
    fn test_push_with_undefined() {
        let mut container: UuidContainer<Uuid4> = UuidContainer::with_capacity(3);
        let uuid1 = Uuid4::generate();
        let uuid2 = Uuid4::generate();

        container.push(uuid1);
        container.push_undefined();
        container.push(uuid2);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&uuid1));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), Some(&uuid2));

        assert!(container.is_defined(0));
        assert!(!container.is_defined(1));
        assert!(container.is_defined(2));
    }

    #[test]
    fn test_extend() {
        let uuid1 = Uuid4::generate();
        let uuid2 = Uuid4::generate();
        let uuid3 = Uuid4::generate();

        let mut container1 = UuidContainer::from_vec(vec![uuid1, uuid2]);
        let container2 = UuidContainer::from_vec(vec![uuid3]);

        container1.extend(&container2).unwrap();

        assert_eq!(container1.len(), 3);
        assert_eq!(container1.get(0), Some(&uuid1));
        assert_eq!(container1.get(1), Some(&uuid2));
        assert_eq!(container1.get(2), Some(&uuid3));
    }

    #[test]
    fn test_extend_from_undefined() {
        let uuid = Uuid7::generate();
        let mut container = UuidContainer::from_vec(vec![uuid]);
        container.extend_from_undefined(2);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&uuid));
        assert_eq!(container.get(1), None); // undefined
        assert_eq!(container.get(2), None); // undefined
    }

    #[test]
    fn test_iter() {
        let uuid1 = Uuid4::generate();
        let uuid2 = Uuid4::generate();
        let uuid3 = Uuid4::generate();
        let uuids = vec![uuid1, uuid2, uuid3];
        let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
        let container = UuidContainer::new(uuids.clone(), bitvec);

        let collected: Vec<Option<Uuid4>> = container.iter().collect();
        assert_eq!(collected, vec![Some(uuids[0]), None, Some(uuids[2])]);
    }

    #[test]
    fn test_slice() {
        let uuids =
            vec![Uuid4::generate(), Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
        let container = UuidContainer::from_vec(uuids.clone());
        let sliced = container.slice(1, 3);

        assert_eq!(sliced.len(), 2);
        assert_eq!(sliced.get(0), Some(&uuids[1]));
        assert_eq!(sliced.get(1), Some(&uuids[2]));
    }

    #[test]
    fn test_filter() {
        let uuids =
            vec![Uuid4::generate(), Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
        let mut container = UuidContainer::from_vec(uuids.clone());
        let mask = BitVec::from_slice(&[true, false, true, false]);

        container.filter(&mask);

        assert_eq!(container.len(), 2);
        assert_eq!(container.get(0), Some(&uuids[0]));
        assert_eq!(container.get(1), Some(&uuids[2]));
    }

    #[test]
    fn test_reorder() {
        let uuids = vec![Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
        let mut container = UuidContainer::from_vec(uuids.clone());
        let indices = [2, 0, 1];

        container.reorder(&indices);

        assert_eq!(container.len(), 3);
        assert_eq!(container.get(0), Some(&uuids[2])); // was index 2
        assert_eq!(container.get(1), Some(&uuids[0])); // was index 0
        assert_eq!(container.get(2), Some(&uuids[1])); // was index 1
    }

    #[test]
    fn test_mixed_uuid_types() {
        // Test that we can have different UUID containers
        let uuid4_container: UuidContainer<Uuid4> =
            UuidContainer::from_vec(vec![Uuid4::generate()]);
        let uuid7_container: UuidContainer<Uuid7> =
            UuidContainer::from_vec(vec![Uuid7::generate()]);

        assert_eq!(uuid4_container.len(), 1);
        assert_eq!(uuid7_container.len(), 1);
    }

    #[test]
    fn test_default() {
        let container: UuidContainer<Uuid4> = UuidContainer::default();
        assert_eq!(container.len(), 0);
        assert!(container.is_empty());
    }
}
