// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct BitVec {
    inner: Arc<BitVecInner>,
}

impl Default for BitVec {
    fn default() -> Self {
        Self { inner: Arc::new(BitVecInner { bits: vec![], len: 0 }) }
    }
}

impl From<&BitVec> for BitVec {
    fn from(value: &BitVec) -> Self {
        value.clone()
    }
}

impl From<Vec<bool>> for BitVec {
    fn from(value: Vec<bool>) -> Self {
        BitVec::from_slice(&value)
    }
}

impl<const N: usize> From<[bool; N]> for BitVec {
    fn from(value: [bool; N]) -> Self {
        BitVec::from_slice(&value)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BitVecInner {
    bits: Vec<u8>,
    len: usize,
}

pub struct BitVecIter {
    inner: Arc<BitVecInner>,
    pos: usize,
}

impl Iterator for BitVecIter {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.inner.len {
            return None;
        }

        let byte = self.inner.bits[self.pos / 8];
        let bit = (byte >> (self.pos % 8)) & 1;
        self.pos += 1;
        Some(bit != 0)
    }
}

impl BitVec {
    pub fn repeat(len: usize, value: bool) -> Self {
        if value {
            BitVec::from_fn(len, |_| true)
        } else {
            let byte_count = (len + 7) / 8;
            BitVec { inner: Arc::new(BitVecInner { bits: vec![0x00; byte_count], len }) }
        }
    }

    pub fn from_slice(slice: &[bool]) -> Self {
        let mut bv = BitVec::repeat(slice.len(), false);
        for i in 0..slice.len() {
            if slice[i] {
                bv.set(i, true);
            }
        }
        bv
    }

    pub fn empty() -> Self {
        Self { inner: Arc::new(BitVecInner { bits: Vec::new(), len: 0 }) }
    }

    pub fn from_fn(len: usize, mut f: impl FnMut(usize) -> bool) -> Self {
        let mut bv = BitVec::repeat(len, false);
        for i in 0..len {
            if f(i) {
                bv.set(i, true);
            }
        }
        bv
    }

    pub fn take(&self, n: usize) -> BitVec {
        let len = n.min(self.inner.len);

        let byte_len = (len + 7) / 8;
        let mut bits = vec![0u8; byte_len];

        for i in 0..len {
            let orig_byte = self.inner.bits[i / 8];
            let bit = (orig_byte >> (i % 8)) & 1;
            if bit != 0 {
                bits[i / 8] |= 1 << (i % 8);
            }
        }

        BitVec { inner: Arc::new(BitVecInner { bits, len }) }
    }

    fn make_mut(&mut self) -> &mut BitVecInner {
        Arc::make_mut(&mut self.inner)
    }

    pub fn extend(&mut self, other: &BitVec) {
        let start_len = self.len();
        let other_len = other.len();
        let total_len = start_len + other_len;
        let total_byte_len = (total_len + 7) / 8;

        let inner = self.make_mut();
        inner.bits.resize(total_byte_len, 0);

        for i in 0..other_len {
            let bit = other.get(i);
            if bit {
                let idx = start_len + i;
                let byte = &mut inner.bits[idx / 8];
                let bit_pos = idx % 8;
                *byte |= 1 << bit_pos;
            }
        }

        inner.len = total_len;
    }

    pub fn push(&mut self, bit: bool) {
        let inner = self.make_mut();
        let byte_index = inner.len / 8;
        let bit_index = inner.len % 8;

        if byte_index >= inner.bits.len() {
            inner.bits.push(0);
        }

        if bit {
            inner.bits[byte_index] |= 1 << bit_index;
        }

        inner.len += 1;
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }

    pub fn capacity(&self) -> usize {
        self.inner.bits.capacity() * 8
    }

    pub fn get(&self, idx: usize) -> bool {
        assert!(idx < self.inner.len);
        let byte = self.inner.bits[idx / 8];
        let bit = idx % 8;
        (byte >> bit) & 1 != 0
    }

    pub fn set(&mut self, idx: usize, value: bool) {
        assert!(idx < self.inner.len);
        let inner = self.make_mut();
        let byte = &mut inner.bits[idx / 8];
        let bit = idx % 8;
        if value {
            *byte |= 1 << bit;
        } else {
            *byte &= !(1 << bit);
        }
    }

    pub fn iter(&self) -> BitVecIter {
        BitVecIter { inner: self.inner.clone(), pos: 0 }
    }

    pub fn and(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());
        let len = self.len();
        let byte_count = (len + 7) / 8;
        let mut result_bits = vec![0u8; byte_count];

        // Process 8 bytes at a time for better performance
        let chunks = byte_count / 8;
        let mut i = 0;

        // Process 64-bit chunks
        for _ in 0..chunks {
            let a = u64::from_le_bytes([
                self.inner.bits[i],
                self.inner.bits[i + 1],
                self.inner.bits[i + 2],
                self.inner.bits[i + 3],
                self.inner.bits[i + 4],
                self.inner.bits[i + 5],
                self.inner.bits[i + 6],
                self.inner.bits[i + 7],
            ]);
            let b = u64::from_le_bytes([
                other.inner.bits[i],
                other.inner.bits[i + 1],
                other.inner.bits[i + 2],
                other.inner.bits[i + 3],
                other.inner.bits[i + 4],
                other.inner.bits[i + 5],
                other.inner.bits[i + 6],
                other.inner.bits[i + 7],
            ]);
            let result = a & b;
            result_bits[i..i + 8].copy_from_slice(&result.to_le_bytes());
            i += 8;
        }

        // Process remaining bytes
        while i < byte_count {
            result_bits[i] = self.inner.bits[i] & other.inner.bits[i];
            i += 1;
        }

        BitVec { inner: Arc::new(BitVecInner { bits: result_bits, len }) }
    }

    pub fn to_vec(&self) -> Vec<bool> {
        self.iter().collect()
    }

    pub fn count_ones(&self) -> usize {
        // Count complete bytes using built-in popcount
        let mut count = self.inner.bits.iter().map(|&byte| byte.count_ones() as usize).sum();

        // Adjust for partial last byte if needed
        let full_bytes = self.inner.len / 8;
        let remainder_bits = self.inner.len % 8;

        if remainder_bits > 0 && full_bytes < self.inner.bits.len() {
            let last_byte = self.inner.bits[full_bytes];
            // Mask out bits beyond our length
            let mask = (1u8 << remainder_bits) - 1;
            // Subtract the invalid bits we counted
            count -= (last_byte & !mask).count_ones() as usize;
        }

        count
    }

    pub fn any(&self) -> bool {
        // Fast path: check if any complete bytes are non-zero
        let full_bytes = self.inner.len / 8;
        for i in 0..full_bytes {
            if self.inner.bits[i] != 0 {
                return true;
            }
        }

        // Check remaining bits in last partial byte
        let remainder_bits = self.inner.len % 8;
        if remainder_bits > 0 && full_bytes < self.inner.bits.len() {
            let last_byte = self.inner.bits[full_bytes];
            let mask = (1u8 << remainder_bits) - 1;
            return (last_byte & mask) != 0;
        }

        false
    }

    pub fn none(&self) -> bool {
        !self.any()
    }

    pub fn or(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());
        let len = self.len();
        let byte_count = (len + 7) / 8;
        let mut result_bits = vec![0u8; byte_count];

        // Process 8 bytes at a time for better performance
        let chunks = byte_count / 8;
        let mut i = 0;

        // Process 64-bit chunks
        for _ in 0..chunks {
            let a = u64::from_le_bytes([
                self.inner.bits[i],
                self.inner.bits[i + 1],
                self.inner.bits[i + 2],
                self.inner.bits[i + 3],
                self.inner.bits[i + 4],
                self.inner.bits[i + 5],
                self.inner.bits[i + 6],
                self.inner.bits[i + 7],
            ]);
            let b = u64::from_le_bytes([
                other.inner.bits[i],
                other.inner.bits[i + 1],
                other.inner.bits[i + 2],
                other.inner.bits[i + 3],
                other.inner.bits[i + 4],
                other.inner.bits[i + 5],
                other.inner.bits[i + 6],
                other.inner.bits[i + 7],
            ]);
            let result = a | b;
            result_bits[i..i + 8].copy_from_slice(&result.to_le_bytes());
            i += 8;
        }

        // Process remaining bytes
        while i < byte_count {
            result_bits[i] = self.inner.bits[i] | other.inner.bits[i];
            i += 1;
        }

        BitVec { inner: Arc::new(BitVecInner { bits: result_bits, len }) }
    }

    pub fn is_owned(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let byte_capacity = (capacity + 7) / 8;
        Self { inner: Arc::new(BitVecInner { bits: Vec::with_capacity(byte_capacity), len: 0 }) }
    }

    pub fn reorder(&mut self, indices: &[usize]) {
        assert_eq!(self.len(), indices.len());
        let len = self.len();
        let byte_count = (len + 7) / 8;
        let mut new_bits = vec![0u8; byte_count];

        // Collect old bit values before mutating
        for (new_idx, &old_idx) in indices.iter().enumerate() {
            if self.get(old_idx) {
                let byte_idx = new_idx / 8;
                let bit_idx = new_idx % 8;
                new_bits[byte_idx] |= 1 << bit_idx;
            }
        }

        // Now mutate
        let inner = self.make_mut();
        inner.bits = new_bits;
    }
}

impl fmt::Display for BitVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for bit in self.iter() {
            write!(f, "{}", if bit { '1' } else { '0' })?;
        }
        Ok(())
    }
}

impl Deref for BitVec {
    type Target = BitVecInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Serialize for BitVec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BitVec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = BitVecInner::deserialize(deserializer)?;
        Ok(BitVec { inner: Arc::new(inner) })
    }
}

#[cfg(test)]
mod tests {
    mod new {
        use crate::util::BitVec;

        #[test]
        fn test_all_false() {
            let bv = BitVec::repeat(10, false);
            assert_eq!(bv.len(), 10);
            for i in 0..10 {
                assert!(!bv.get(i), "expected bit {} to be false", i);
            }
        }

        #[test]
        fn test_all_true() {
            let bv = BitVec::repeat(10, true);
            assert_eq!(bv.len(), 10);
            for i in 0..10 {
                assert!(bv.get(i), "expected bit {} to be true", i);
            }
        }
    }

    mod get_and_set {
        use crate::util::BitVec;

        #[test]
        fn test_ok() {
            let mut bv = BitVec::repeat(16, false);
            bv.set(3, true);
            bv.set(7, true);
            bv.set(15, true);

            assert!(bv.get(3));
            assert!(bv.get(7));
            assert!(bv.get(15));
            assert!(!bv.get(0));
            assert!(!bv.get(14));
        }

        #[test]
        #[should_panic(expected = "assertion failed")]
        fn test_get_out_of_bounds() {
            let bv = BitVec::repeat(8, false);
            bv.get(8);
        }

        #[test]
        #[should_panic(expected = "assertion failed")]
        fn test_set_out_of_bounds() {
            let mut bv = BitVec::repeat(8, false);
            bv.set(8, true);
        }
    }

    mod from_fn {
        use crate::util::BitVec;

        #[test]
        fn test_ok() {
            let bv = BitVec::from_fn(10, |i| i % 2 == 0);
            for i in 0..10 {
                assert_eq!(bv.get(i), i % 2 == 0, "bit {} mismatch", i);
            }
        }
    }

    mod iter {
        use crate::util::BitVec;

        #[test]
        fn test_ok() {
            let bv = BitVec::from_fn(4, |i| i % 2 == 0);
            let collected: Vec<bool> = bv.iter().collect();
            assert_eq!(collected, vec![true, false, true, false]);
        }

        #[test]
        fn test_empty() {
            let bv = BitVec::from_fn(0, |i| i % 2 == 0);
            let collected: Vec<bool> = bv.iter().collect();
            assert_eq!(collected, vec![]);
        }
    }

    mod and {
        use crate::util::BitVec;

        #[test]
        fn test_ok() {
            let a = BitVec::from_fn(8, |i| i % 2 == 0); // 10101010
            let b = BitVec::from_fn(8, |i| i < 4); // 11110000
            let result = a.and(&b); // 10100000
            let expected = [true, false, true, false, false, false, false, false];
            for i in 0..8 {
                assert_eq!(result.get(i), expected[i], "mismatch at bit {}", i);
            }
        }
    }

    mod from_slice {
        use crate::util::BitVec;

        #[test]
        fn test_empty_slice() {
            let bv = BitVec::from_slice(&[]);
            assert_eq!(bv.len(), 0);
        }

        #[test]
        fn test_single_bit() {
            let bv = BitVec::from_slice(&[true]);
            assert_eq!(bv.len(), 1);
            assert!(bv.get(0));

            let bv = BitVec::from_slice(&[false]);
            assert_eq!(bv.len(), 1);
            assert!(!bv.get(0));
        }

        #[test]
        fn test_multiple_bits() {
            let bv = BitVec::from_slice(&[true, false, true, false, true]);
            assert_eq!(bv.len(), 5);
            assert!(bv.get(0));
            assert!(!bv.get(1));
            assert!(bv.get(2));
            assert!(!bv.get(3));
            assert!(bv.get(4));
        }

        #[test]
        fn test_cross_byte_boundary() {
            let input = [true, false, true, false, true, false, true, false, true];
            let bv = BitVec::from_slice(&input);
            assert_eq!(bv.len(), 9);
            for i in 0..9 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_large_slice() {
            let input: Vec<bool> = (0..1000).map(|i| i % 3 == 0).collect();
            let bv = BitVec::from_slice(&input);
            assert_eq!(bv.len(), 1000);
            for i in 0..1000 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }
    }

    mod from_array {
        use crate::util::BitVec;

        #[test]
        fn test_from_array_1() {
            let bv = BitVec::from([true]);
            assert_eq!(bv.len(), 1);
            assert!(bv.get(0));
        }

        #[test]
        fn test_from_array_2() {
            let bv = BitVec::from([true, false]);
            assert_eq!(bv.len(), 2);
            assert!(bv.get(0));
            assert!(!bv.get(1));
        }

        #[test]
        fn test_from_array_4() {
            let bv = BitVec::from([true, false, true, false]);
            assert_eq!(bv.len(), 4);
            assert!(bv.get(0));
            assert!(!bv.get(1));
            assert!(bv.get(2));
            assert!(!bv.get(3));
        }

        #[test]
        fn test_from_array_large() {
            let bv = BitVec::from([true; 16]);
            assert_eq!(bv.len(), 16);
            for i in 0..16 {
                assert!(bv.get(i), "expected bit {} to be true", i);
            }
        }

        #[test]
        fn test_from_array_cross_byte() {
            let bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
            assert_eq!(bv.len(), 9);
            for i in 0..9 {
                assert_eq!(bv.get(i), i % 2 == 0, "mismatch at bit {}", i);
            }
        }
    }

    mod from_vec {
        use crate::util::BitVec;

        #[test]
        fn test_from_vec_empty() {
            let bv = BitVec::from(Vec::<bool>::new());
            assert_eq!(bv.len(), 0);
        }

        #[test]
        fn test_from_vec_small() {
            let bv = BitVec::from(vec![true, false, true]);
            assert_eq!(bv.len(), 3);
            assert!(bv.get(0));
            assert!(!bv.get(1));
            assert!(bv.get(2));
        }

        #[test]
        fn test_from_vec_large() {
            let input: Vec<bool> = (0..100).map(|i| i % 7 == 0).collect();
            let bv = BitVec::from(input.clone());
            assert_eq!(bv.len(), 100);
            for i in 0..100 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }
    }

    mod empty {
        use crate::util::BitVec;

        #[test]
        fn test_empty() {
            let bv = BitVec::empty();
            assert_eq!(bv.len(), 0);
            assert!(bv.none());
            assert!(!bv.any());
            assert_eq!(bv.count_ones(), 0);
        }

        #[test]
        fn test_empty_operations() {
            let mut bv = BitVec::empty();

            // Push operations should work
            bv.push(true);
            assert_eq!(bv.len(), 1);
            assert!(bv.get(0));

            // Extend should work
            let other = BitVec::from([false, true]);
            bv.extend(&other);
            assert_eq!(bv.len(), 3);
            assert!(bv.get(0));
            assert!(!bv.get(1));
            assert!(bv.get(2));
        }
    }

    mod take {
        use crate::util::BitVec;

        #[test]
        fn test_take_empty() {
            let bv = BitVec::empty();
            let taken = bv.take(5);
            assert_eq!(taken.len(), 0);
        }

        #[test]
        fn test_take_less_than_available() {
            let bv = BitVec::from([true, false, true, false, true]);
            let taken = bv.take(3);
            assert_eq!(taken.len(), 3);
            assert!(taken.get(0));
            assert!(!taken.get(1));
            assert!(taken.get(2));
        }

        #[test]
        fn test_take_exact_length() {
            let bv = BitVec::from([true, false, true]);
            let taken = bv.take(3);
            assert_eq!(taken.len(), 3);
            assert!(taken.get(0));
            assert!(!taken.get(1));
            assert!(taken.get(2));
        }

        #[test]
        fn test_take_more_than_available() {
            let bv = BitVec::from([true, false]);
            let taken = bv.take(5);
            assert_eq!(taken.len(), 2);
            assert!(taken.get(0));
            assert!(!taken.get(1));
        }

        #[test]
        fn test_take_zero() {
            let bv = BitVec::from([true, false, true]);
            let taken = bv.take(0);
            assert_eq!(taken.len(), 0);
        }

        #[test]
        fn test_take_cross_byte_boundary() {
            let bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
            let taken = bv.take(6);
            assert_eq!(taken.len(), 6);
            for i in 0..6 {
                assert_eq!(taken.get(i), i % 2 == 0, "mismatch at bit {}", i);
            }
        }
    }

    mod extend {
        use crate::util::BitVec;

        #[test]
        fn test_extend_empty_to_empty() {
            let mut bv1 = BitVec::empty();
            let bv2 = BitVec::empty();
            bv1.extend(&bv2);
            assert_eq!(bv1.len(), 0);
        }

        #[test]
        fn test_extend_empty_to_nonempty() {
            let mut bv1 = BitVec::from([true, false]);
            let bv2 = BitVec::empty();
            bv1.extend(&bv2);
            assert_eq!(bv1.len(), 2);
            assert!(bv1.get(0));
            assert!(!bv1.get(1));
        }

        #[test]
        fn test_extend_nonempty_to_empty() {
            let mut bv1 = BitVec::empty();
            let bv2 = BitVec::from([true, false]);
            bv1.extend(&bv2);
            assert_eq!(bv1.len(), 2);
            assert!(bv1.get(0));
            assert!(!bv1.get(1));
        }

        #[test]
        fn test_extend_basic() {
            let mut bv1 = BitVec::from([true, false]);
            let bv2 = BitVec::from([false, true]);
            bv1.extend(&bv2);
            assert_eq!(bv1.len(), 4);
            assert!(bv1.get(0));
            assert!(!bv1.get(1));
            assert!(!bv1.get(2));
            assert!(bv1.get(3));
        }

        #[test]
        fn test_extend_cross_byte_boundary() {
            let mut bv1 = BitVec::from([true, false, true, false, true, false]);
            let bv2 = BitVec::from([false, true, false]);
            bv1.extend(&bv2);
            assert_eq!(bv1.len(), 9);

            let expected = [true, false, true, false, true, false, false, true, false];
            for i in 0..9 {
                assert_eq!(bv1.get(i), expected[i], "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_extend_large() {
            let mut bv1 = BitVec::from_fn(50, |i| i % 2 == 0);
            let bv2 = BitVec::from_fn(50, |i| i % 3 == 0);
            bv1.extend(&bv2);
            assert_eq!(bv1.len(), 100);

            for i in 0..50 {
                assert_eq!(bv1.get(i), i % 2 == 0, "first half mismatch at bit {}", i);
            }
            for i in 50..100 {
                assert_eq!(bv1.get(i), (i - 50) % 3 == 0, "second half mismatch at bit {}", i);
            }
        }
    }

    mod push {
        use crate::util::BitVec;

        #[test]
        fn test_push_to_empty() {
            let mut bv = BitVec::empty();
            bv.push(true);
            assert_eq!(bv.len(), 1);
            assert!(bv.get(0));
        }

        #[test]
        fn test_push_alternating() {
            let mut bv = BitVec::empty();
            for i in 0..10 {
                bv.push(i % 2 == 0);
            }
            assert_eq!(bv.len(), 10);
            for i in 0..10 {
                assert_eq!(bv.get(i), i % 2 == 0, "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_push_cross_byte_boundary() {
            let mut bv = BitVec::empty();
            for i in 0..17 {
                bv.push(i % 3 == 0);
            }
            assert_eq!(bv.len(), 17);
            for i in 0..17 {
                assert_eq!(bv.get(i), i % 3 == 0, "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_push_many() {
            let mut bv = BitVec::empty();
            for i in 0..1000 {
                bv.push(i % 7 == 0);
            }
            assert_eq!(bv.len(), 1000);
            for i in 0..1000 {
                assert_eq!(bv.get(i), i % 7 == 0, "mismatch at bit {}", i);
            }
        }
    }

    mod reorder {
        use crate::util::BitVec;

        #[test]
        fn test_reorder_identity() {
            let mut bv = BitVec::from([true, false, true, false]);
            bv.reorder(&[0, 1, 2, 3]);
            assert_eq!(bv.len(), 4);
            assert!(bv.get(0));
            assert!(!bv.get(1));
            assert!(bv.get(2));
            assert!(!bv.get(3));
        }

        #[test]
        fn test_reorder_reverse() {
            let mut bv = BitVec::from([true, false, true, false]);
            bv.reorder(&[3, 2, 1, 0]);
            assert_eq!(bv.len(), 4);
            assert!(!bv.get(0)); // was index 3
            assert!(bv.get(1)); // was index 2
            assert!(!bv.get(2)); // was index 1
            assert!(bv.get(3)); // was index 0
        }

        #[test]
        fn test_reorder_custom() {
            let mut bv = BitVec::from([true, false, true, false]);
            bv.reorder(&[2, 0, 3, 1]);
            assert_eq!(bv.len(), 4);
            assert!(bv.get(0)); // was index 2
            assert!(bv.get(1)); // was index 0
            assert!(!bv.get(2)); // was index 3
            assert!(!bv.get(3)); // was index 1
        }

        #[test]
        fn test_reorder_cross_byte_boundary() {
            let mut bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
            bv.reorder(&[8, 7, 6, 5, 4, 3, 2, 1, 0]);
            assert_eq!(bv.len(), 9);

            let expected = [true, false, true, false, true, false, true, false, true]; // reversed
            for i in 0..9 {
                assert_eq!(bv.get(i), expected[8 - i], "mismatch at bit {}", i);
            }
        }

        #[test]
        #[should_panic(expected = "assertion `left == right` failed")]
        fn test_reorder_wrong_length() {
            let mut bv = BitVec::from([true, false, true]);
            bv.reorder(&[0, 1]); // Wrong length should panic
        }
    }

    mod count_ones {
        use crate::util::BitVec;

        #[test]
        fn test_count_ones_empty() {
            let bv = BitVec::empty();
            assert_eq!(bv.count_ones(), 0);
        }

        #[test]
        fn test_count_ones_all_false() {
            let bv = BitVec::repeat(10, false);
            assert_eq!(bv.count_ones(), 0);
        }

        #[test]
        fn test_count_ones_all_true() {
            let bv = BitVec::repeat(10, true);
            assert_eq!(bv.count_ones(), 10);
        }

        #[test]
        fn test_count_ones_mixed() {
            let bv = BitVec::from([true, false, true, false, true]);
            assert_eq!(bv.count_ones(), 3);
        }

        #[test]
        fn test_count_ones_alternating() {
            let bv = BitVec::from_fn(100, |i| i % 2 == 0);
            assert_eq!(bv.count_ones(), 50);
        }

        #[test]
        fn test_count_ones_cross_byte_boundary() {
            let bv = BitVec::from_fn(17, |i| i % 3 == 0);
            let expected = (0..17).filter(|&i| i % 3 == 0).count();
            assert_eq!(bv.count_ones(), expected);
        }
    }

    mod any_none {
        use crate::util::BitVec;

        #[test]
        fn test_any_none_empty() {
            let bv = BitVec::empty();
            assert!(!bv.any());
            assert!(bv.none());
        }

        #[test]
        fn test_any_none_all_false() {
            let bv = BitVec::repeat(10, false);
            assert!(!bv.any());
            assert!(bv.none());
        }

        #[test]
        fn test_any_none_all_true() {
            let bv = BitVec::repeat(10, true);
            assert!(bv.any());
            assert!(!bv.none());
        }

        #[test]
        fn test_any_none_mixed() {
            let bv = BitVec::from([false, false, true, false]);
            assert!(bv.any());
            assert!(!bv.none());
        }

        #[test]
        fn test_any_none_single_true() {
            let bv = BitVec::from([true]);
            assert!(bv.any());
            assert!(!bv.none());
        }

        #[test]
        fn test_any_none_single_false() {
            let bv = BitVec::from([false]);
            assert!(!bv.any());
            assert!(bv.none());
        }
    }

    mod to_vec {
        use crate::util::BitVec;

        #[test]
        fn test_to_vec_empty() {
            let bv = BitVec::empty();
            assert_eq!(bv.to_vec(), Vec::<bool>::new());
        }

        #[test]
        fn test_to_vec_small() {
            let bv = BitVec::from([true, false, true]);
            assert_eq!(bv.to_vec(), vec![true, false, true]);
        }

        #[test]
        fn test_to_vec_cross_byte_boundary() {
            let input = [true, false, true, false, true, false, true, false, true];
            let bv = BitVec::from(input);
            assert_eq!(bv.to_vec(), input.to_vec());
        }

        #[test]
        fn test_to_vec_large() {
            let input: Vec<bool> = (0..100).map(|i| i % 3 == 0).collect();
            let bv = BitVec::from(input.clone());
            assert_eq!(bv.to_vec(), input);
        }
    }

    mod display {
        use crate::util::BitVec;

        #[test]
        fn test_display_empty() {
            let bv = BitVec::empty();
            assert_eq!(format!("{}", bv), "");
        }

        #[test]
        fn test_display_small() {
            let bv = BitVec::from([true, false, true]);
            assert_eq!(format!("{}", bv), "101");
        }

        #[test]
        fn test_display_all_false() {
            let bv = BitVec::repeat(5, false);
            assert_eq!(format!("{}", bv), "00000");
        }

        #[test]
        fn test_display_all_true() {
            let bv = BitVec::repeat(5, true);
            assert_eq!(format!("{}", bv), "11111");
        }

        #[test]
        fn test_display_cross_byte_boundary() {
            let bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
            assert_eq!(format!("{}", bv), "101010101");
        }
    }

    mod and_operation {
        use crate::util::BitVec;

        #[test]
        fn test_and_empty() {
            let a = BitVec::empty();
            let b = BitVec::empty();
            let result = a.and(&b);
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_and_all_true() {
            let a = BitVec::repeat(5, true);
            let b = BitVec::repeat(5, true);
            let result = a.and(&b);
            assert_eq!(result.len(), 5);
            for i in 0..5 {
                assert!(result.get(i), "expected bit {} to be true", i);
            }
        }

        #[test]
        fn test_and_all_false() {
            let a = BitVec::repeat(5, false);
            let b = BitVec::repeat(5, false);
            let result = a.and(&b);
            assert_eq!(result.len(), 5);
            for i in 0..5 {
                assert!(!result.get(i), "expected bit {} to be false", i);
            }
        }

        #[test]
        fn test_and_mixed() {
            let a = BitVec::from([true, true, false, false]);
            let b = BitVec::from([true, false, true, false]);
            let result = a.and(&b);
            assert_eq!(result.len(), 4);
            assert!(result.get(0)); // true & true = true
            assert!(!result.get(1)); // true & false = false
            assert!(!result.get(2)); // false & true = false
            assert!(!result.get(3)); // false & false = false
        }

        #[test]
        fn test_and_cross_byte_boundary() {
            let a = BitVec::from_fn(17, |i| i % 2 == 0);
            let b = BitVec::from_fn(17, |i| i % 3 == 0);
            let result = a.and(&b);
            assert_eq!(result.len(), 17);
            for i in 0..17 {
                let expected = (i % 2 == 0) && (i % 3 == 0);
                assert_eq!(result.get(i), expected, "mismatch at bit {}", i);
            }
        }

        #[test]
        #[should_panic(expected = "assertion `left == right` failed")]
        fn test_and_different_lengths() {
            let a = BitVec::repeat(3, true);
            let b = BitVec::repeat(5, true);
            a.and(&b); // Should panic due to different lengths
        }
    }

    mod edge_cases {
        use crate::util::BitVec;

        #[test]
        fn test_single_bit_operations() {
            let mut bv = BitVec::from([true]);
            assert_eq!(bv.len(), 1);
            assert!(bv.get(0));
            assert_eq!(bv.count_ones(), 1);
            assert!(bv.any());
            assert!(!bv.none());

            bv.set(0, false);
            assert!(!bv.get(0));
            assert_eq!(bv.count_ones(), 0);
            assert!(!bv.any());
            assert!(bv.none());
        }

        #[test]
        fn test_exactly_one_byte() {
            let input = [true, false, true, false, true, false, true, false];
            let bv = BitVec::from(input);
            assert_eq!(bv.len(), 8);
            for i in 0..8 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_exactly_multiple_bytes() {
            let input: Vec<bool> = (0..16).map(|i| i % 2 == 0).collect();
            let bv = BitVec::from(input.clone());
            assert_eq!(bv.len(), 16);
            for i in 0..16 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_one_bit_past_byte_boundary() {
            let input: Vec<bool> = (0..9).map(|i| i % 2 == 0).collect();
            let bv = BitVec::from(input.clone());
            assert_eq!(bv.len(), 9);
            for i in 0..9 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_seven_bits_in_byte() {
            let input = [true, false, true, false, true, false, true];
            let bv = BitVec::from(input);
            assert_eq!(bv.len(), 7);
            for i in 0..7 {
                assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
            }
        }
    }

    mod cow_behavior {
        use crate::util::BitVec;

        #[test]
        fn test_is_owned() {
            let mut owned = BitVec::with_capacity(16);
            owned.push(true);
            owned.push(false);

            assert!(owned.is_owned());

            let shared = owned.clone();
            assert!(!owned.is_owned());
            assert!(!shared.is_owned());

            drop(shared);

            assert!(owned.is_owned());
        }

        #[test]
        fn test_is_shared() {
            let mut owned = BitVec::with_capacity(16);
            owned.push(true);
            owned.push(false);

            assert!(!owned.is_shared());

            let shared = owned.clone();
            assert!(owned.is_shared());
            assert!(shared.is_shared());

            drop(shared);

            assert!(!owned.is_shared());
        }

        #[test]
        fn test_push_cow() {
            let mut owned = BitVec::with_capacity(16);
            owned.push(true);
            owned.push(false);

            let ptr_before_owned = ptr_of(&owned);
            owned.push(true);
            assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
            assert_eq!(owned.len(), 3);

            let mut shared = owned.clone();

            let ptr_before_shared = ptr_of(&shared);
            shared.push(true);
            assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
            assert_eq!(owned.len(), 3);
            assert_eq!(shared.len(), 4);
        }

        #[test]
        fn test_set_cow() {
            let mut owned = BitVec::repeat(8, false);
            owned.set(1, true);

            let ptr_before_owned = ptr_of(&owned);
            owned.set(2, true);
            assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy

            let mut shared = owned.clone();

            let ptr_before_shared = ptr_of(&shared);
            shared.set(3, true);
            assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
            assert!(!owned.get(3)); // original unchanged
            assert!(shared.get(3)); // new value set
        }

        #[test]
        fn test_extend_cow() {
            let mut owned = BitVec::repeat(4, false);
            let extension = BitVec::repeat(4, true);

            let ptr_before_owned = ptr_of(&owned);
            owned.extend(&extension);
            assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
            assert_eq!(owned.len(), 8);

            let mut shared = owned.clone();

            let ptr_before_shared = ptr_of(&shared);
            shared.extend(&extension);
            assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
            assert_eq!(owned.len(), 8);
            assert_eq!(shared.len(), 12);
        }

        #[test]
        fn test_reorder_cow() {
            let mut owned = BitVec::from_fn(4, |i| i % 2 == 0);

            // reorder always creates new bits array even if owned
            owned.reorder(&[1, 0, 3, 2]);

            let mut shared = owned.clone();

            let ptr_before_shared = ptr_of(&shared);
            shared.reorder(&[0, 1, 2, 3]); // identity reorder
            assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
        }

        fn ptr_of(v: &BitVec) -> *const u8 {
            v.inner.bits.as_ptr()
        }
    }

    mod stress_tests {
        use crate::util::BitVec;

        #[test]
        fn test_large_bitvec_operations() {
            let size = 10000;
            let mut bv = BitVec::empty();

            // Test large push operations
            for i in 0..size {
                bv.push(i % 17 == 0);
            }
            assert_eq!(bv.len(), size);

            // Verify all values
            for i in 0..size {
                assert_eq!(bv.get(i), i % 17 == 0, "mismatch at bit {}", i);
            }

            // Test count_ones on large bitvec
            let expected_ones = (0..size).filter(|&i| i % 17 == 0).count();
            assert_eq!(bv.count_ones(), expected_ones);
        }

        #[test]
        fn test_large_extend_operations() {
            let size = 5000;
            let mut bv1 = BitVec::from_fn(size, |i| i % 13 == 0);
            let bv2 = BitVec::from_fn(size, |i| i % 19 == 0);

            bv1.extend(&bv2);
            assert_eq!(bv1.len(), size * 2);

            // Verify first half
            for i in 0..size {
                assert_eq!(bv1.get(i), i % 13 == 0, "first half mismatch at bit {}", i);
            }

            // Verify second half
            for i in size..(size * 2) {
                assert_eq!(bv1.get(i), (i - size) % 19 == 0, "second half mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_many_byte_boundaries() {
            // Test various sizes around byte boundaries
            for size in [7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129] {
                let bv = BitVec::from_fn(size, |i| i % 3 == 0);
                assert_eq!(bv.len(), size);

                for i in 0..size {
                    assert_eq!(bv.get(i), i % 3 == 0, "size {} mismatch at bit {}", size, i);
                }

                let expected_ones = (0..size).filter(|&i| i % 3 == 0).count();
                assert_eq!(bv.count_ones(), expected_ones, "count_ones mismatch for size {}", size);
            }
        }

        #[test]
        fn test_multiple_and_operations() {
            let size = 1000;
            let a = BitVec::from_fn(size, |i| i % 2 == 0);
            let b = BitVec::from_fn(size, |i| i % 3 == 0);
            let c = BitVec::from_fn(size, |i| i % 5 == 0);

            let ab = a.and(&b);
            let abc = ab.and(&c);

            assert_eq!(abc.len(), size);
            for i in 0..size {
                let expected = (i % 2 == 0) && (i % 3 == 0) && (i % 5 == 0);
                assert_eq!(abc.get(i), expected, "mismatch at bit {}", i);
            }
        }

        #[test]
        fn test_complex_reorder_pattern() {
            let size = 100;
            let mut bv = BitVec::from_fn(size, |i| i % 7 == 0);

            // Create a complex reordering pattern
            let mut indices: Vec<usize> = (0..size).collect();
            indices.reverse();

            let original_values: Vec<bool> = bv.to_vec();
            bv.reorder(&indices);

            // Verify reordering worked correctly
            for i in 0..size {
                let original_index = indices[i];
                assert_eq!(
                    bv.get(i),
                    original_values[original_index],
                    "reorder mismatch at position {}",
                    i
                );
            }
        }
    }

    mod property_based_tests {
        use crate::util::BitVec;

        #[test]
        fn test_roundtrip_conversions() {
            // Test various patterns
            let patterns = [
                vec![],
                vec![true],
                vec![false],
                vec![true, false],
                vec![false, true],
                (0..50).map(|i| i % 2 == 0).collect::<Vec<_>>(),
                (0..50).map(|i| i % 3 == 0).collect::<Vec<_>>(),
                (0..100).map(|i| i % 7 == 0).collect::<Vec<_>>(),
            ];

            for pattern in patterns {
                // Test Vec<bool> -> BitVec -> Vec<bool>
                let bv = BitVec::from(pattern.clone());
                let result = bv.to_vec();
                assert_eq!(
                    pattern,
                    result,
                    "roundtrip failed for pattern length {}",
                    pattern.len()
                );

                // Test slice -> BitVec -> Vec<bool>
                let bv2 = BitVec::from_slice(&pattern);
                let result2 = bv2.to_vec();
                assert_eq!(
                    pattern,
                    result2,
                    "slice roundtrip failed for pattern length {}",
                    pattern.len()
                );

                if pattern.len() <= 32 {
                    // Test array -> BitVec for small patterns
                    let bv3 = BitVec::from_slice(&pattern);
                    assert_eq!(bv3.len(), pattern.len());
                    for (i, &expected) in pattern.iter().enumerate() {
                        assert_eq!(bv3.get(i), expected, "array conversion mismatch at bit {}", i);
                    }
                }
            }
        }

        #[test]
        fn test_invariants() {
            let patterns =
                [vec![], vec![true], vec![false], (0..100).map(|i| i % 5 == 0).collect::<Vec<_>>()];

            for pattern in patterns {
                let bv = BitVec::from(pattern.clone());

                // Length invariant
                assert_eq!(bv.len(), pattern.len());

                // count_ones + count_zeros = len
                let count_ones = bv.count_ones();
                let count_zeros = pattern.iter().filter(|&&b| !b).count();
                assert_eq!(count_ones + count_zeros, pattern.len());

                // any() and none() consistency
                if count_ones > 0 {
                    assert!(bv.any());
                    assert!(!bv.none());
                } else {
                    assert!(!bv.any());
                    assert!(bv.none());
                }

                // get() consistency
                for (i, &expected) in pattern.iter().enumerate() {
                    assert_eq!(bv.get(i), expected, "get() inconsistency at bit {}", i);
                }
            }
        }

        #[test]
        fn test_extend_preserves_original() {
            let original = BitVec::from([true, false, true]);
            let extension = BitVec::from([false, true]);

            let mut extended = original.clone();
            extended.extend(&extension);

            // Original should be unchanged
            assert_eq!(original.len(), 3);
            assert!(original.get(0));
            assert!(!original.get(1));
            assert!(original.get(2));

            // Extended should have both parts
            assert_eq!(extended.len(), 5);
            assert!(extended.get(0));
            assert!(!extended.get(1));
            assert!(extended.get(2));
            assert!(!extended.get(3));
            assert!(extended.get(4));
        }

        #[test]
        fn test_and_operation_properties() {
            let a = BitVec::from([true, true, false, false]);
            let b = BitVec::from([true, false, true, false]);

            let result = a.and(&b);

            // AND result should never have more ones than either input
            assert!(result.count_ones() <= a.count_ones());
            assert!(result.count_ones() <= b.count_ones());

            // AND is commutative
            let result2 = b.and(&a);
            assert_eq!(result.to_vec(), result2.to_vec());

            // AND with self equals self
            let self_and = a.and(&a);
            assert_eq!(a.to_vec(), self_and.to_vec());
        }
    }
}
