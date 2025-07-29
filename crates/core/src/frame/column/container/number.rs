// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::IsNumber;
use crate::{BitVec, CowVec};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumberContainer<T>
where
    T: IsNumber,
{
    values: CowVec<T>,
    bitvec: BitVec,
}

impl<T> NumberContainer<T>
where
    T: IsNumber + Clone + Debug + Default,
{
    pub fn new(values: Vec<T>, bitvec: BitVec) -> Self {
        debug_assert_eq!(values.len(), bitvec.len());
        Self { values: CowVec::new(values), bitvec }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { values: CowVec::with_capacity(capacity), bitvec: BitVec::with_capacity(capacity) }
    }

    pub fn from_vec(values: Vec<T>) -> Self {
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

    pub fn push(&mut self, value: T) {
        self.values.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.values.push(T::default());
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.len() && self.bitvec.get(index) { self.values.get(index) } else { None }
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
        let new_values: Vec<T> =
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
                new_values.push(T::default());
                new_bitvec.push(false);
            }
        }

        self.values = CowVec::new(new_values);
        self.bitvec = new_bitvec;
    }

    pub fn push_with_convert<U>(&mut self, value: U, converter: impl FnOnce(U) -> Option<T>) {
        match converter(value) {
            Some(v) => {
                self.values.push(v);
                self.bitvec.push(true);
            }
            None => {
                self.values.push(T::default());
                self.bitvec.push(false);
            }
        }
    }
}
