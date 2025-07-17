// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct BitVec {
    inner: Arc<BitVecInner>,
}

#[derive(Clone, Debug)]
pub struct BitVecInner {
    bits: Vec<u8>,
    len: usize,
}

impl BitVec {
    pub fn new(len: usize, value: bool) -> Self {
        let byte_count = (len + 7) / 8;
        let fill_byte = if value { 0xFF } else { 0x00 };
        BitVec {
            inner: Arc::new(BitVecInner {
                bits: vec![fill_byte; byte_count],
                len,
            }),
        }
    }

    pub fn empty() -> Self {
        Self {
            inner: Arc::new(BitVecInner {
                bits: Vec::new(),
                len: 0,
            }),
        }
    }

    pub fn from_fn(len: usize, mut f: impl FnMut(usize) -> bool) -> Self {
        let mut bv = BitVec::new(len, false);
        for i in 0..len {
            if f(i) {
                bv.set(i, true);
            }
        }
        bv
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

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }

    pub fn and(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());
        let len = self.len();
        let byte_count = (len + 7) / 8;
        let mut result_bits = vec![0u8; byte_count];
        
        for i in 0..byte_count {
            result_bits[i] = self.inner.bits[i] & other.inner.bits[i];
        }
        
        BitVec {
            inner: Arc::new(BitVecInner {
                bits: result_bits,
                len,
            }),
        }
    }

    pub fn count_ones(&self) -> usize {
        let mut result = 0;
        for i in 0..self.len() {
            if self.get(i) {
                result += 1;
            }
        }
        result
    }

    pub fn any(&self) -> bool {
        self.count_ones() > 0
    }

    pub fn none(&self) -> bool {
        self.count_ones() == 0
    }

    pub fn is_owned(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let byte_capacity = (capacity + 7) / 8;
        Self {
            inner: Arc::new(BitVecInner {
                bits: Vec::with_capacity(byte_capacity),
                len: 0,
            }),
        }
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

#[cfg(test)]
mod tests {
    mod new {
        use crate::util::BitVec;

        #[test]
        fn test_all_false() {
            let bv = BitVec::new(10, false);
            assert_eq!(bv.len(), 10);
            for i in 0..10 {
                assert!(!bv.get(i), "expected bit {} to be false", i);
            }
        }

        #[test]
        fn test_all_true() {
            let bv = BitVec::new(10, true);
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
            let mut bv = BitVec::new(16, false);
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
            let bv = BitVec::new(8, false);
            bv.get(8);
        }

        #[test]
        #[should_panic(expected = "assertion failed")]
        fn test_set_out_of_bounds() {
            let mut bv = BitVec::new(8, false);
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
            let mut owned = BitVec::new(8, false);
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
            let mut owned = BitVec::new(4, false);
            let extension = BitVec::new(4, true);
            
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
}
