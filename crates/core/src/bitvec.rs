// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt;

#[derive(Clone, Debug)]
pub struct BitVec {
    bits: Vec<u8>,
    len: usize,
}

impl BitVec {
    pub fn new(len: usize, value: bool) -> Self {
        let byte_count = (len + 7) / 8;
        let fill_byte = if value { 0xFF } else { 0x00 };
        BitVec { bits: vec![fill_byte; byte_count], len }
    }

    pub fn empty() -> Self {
        Self { bits: Vec::new(), len: 0 }
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

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, idx: usize) -> bool {
        assert!(idx < self.len);
        let byte = self.bits[idx / 8];
        let bit = idx % 8;
        (byte >> bit) & 1 != 0
    }

    pub fn set(&mut self, idx: usize, value: bool) {
        assert!(idx < self.len);
        let byte = &mut self.bits[idx / 8];
        let bit = idx % 8;
        if value {
            *byte |= 1 << bit;
        } else {
            *byte &= !(1 << bit);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len).map(|i| self.get(i))
    }

    pub fn and(&self, other: &Self) -> Self {
        assert_eq!(self.len, other.len);
        let mut result = BitVec::new(self.len, false);
        for i in 0..self.bits.len() {
            result.bits[i] = self.bits[i] & other.bits[i];
        }
        result
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
}

impl fmt::Display for BitVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for bit in self.iter() {
            write!(f, "{}", if bit { '1' } else { '0' })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    mod new {
        use crate::BitVec;

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
        use crate::BitVec;

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
        use crate::BitVec;

        #[test]
        fn test_ok() {
            let bv = BitVec::from_fn(10, |i| i % 2 == 0);
            for i in 0..10 {
                assert_eq!(bv.get(i), i % 2 == 0, "bit {} mismatch", i);
            }
        }
    }

    mod iter {
        use crate::BitVec;

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
        use crate::BitVec;

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
}
