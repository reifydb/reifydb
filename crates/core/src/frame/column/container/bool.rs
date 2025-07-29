// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::BitVec;
use serde::{Deserialize, Serialize};

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
        if index < self.len() && self.bitvec.get(index) {
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

    pub fn values(&self) -> &BitVec {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut BitVec {
        &mut self.values
    }

    pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
        self.values.extend(&other.values);
        self.bitvec.extend(&other.bitvec);
        Ok(())
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
}

impl Default for BoolContainer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}
