// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{BitVec, CowVec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StringContainer {
    values: CowVec<String>,
    bitvec: BitVec,
}

impl StringContainer {
    pub fn new(values: Vec<String>, bitvec: BitVec) -> Self {
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

    pub fn from_vec(values: Vec<String>) -> Self {
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

    pub fn push(&mut self, value: String) {
        self.values.push(value);
        self.bitvec.push(true);
    }

    pub fn push_undefined(&mut self) {
        self.values.push(String::new());
        self.bitvec.push(false);
    }

    pub fn get(&self, index: usize) -> Option<&String> {
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

    pub fn values(&self) -> &CowVec<String> {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut CowVec<String> {
        &mut self.values
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
        let new_values: Vec<String> = self.values.iter().skip(start).take(end - start).cloned().collect();
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
                new_values.push(String::new());
                new_bitvec.push(false);
            }
        }
        
        self.values = CowVec::new(new_values);
        self.bitvec = new_bitvec;
    }
}

impl Default for StringContainer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}