// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ordered_float::OrderedFloatError;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

#[repr(transparent)]
#[derive(Copy, Clone, Default)]
pub struct OrderedF32(pub f32);

impl PartialEq for OrderedF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedF32 {}

impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.0.to_bits() ^ ((self.0.to_bits() >> 31) & 0x7fffffff);
        let b = other.0.to_bits() ^ ((other.0.to_bits() >> 31) & 0x7fffffff);
        a.cmp(&b)
    }
}

impl Hash for OrderedF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
         self.0.to_bits().hash(state);
    }
}

impl fmt::Debug for OrderedF32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<OrderedF32> for f32 {
    fn from(v: OrderedF32) -> Self {
        v.0
    }
}

impl TryFrom<f32> for OrderedF32 {
    type Error = OrderedFloatError;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        let normalized = if f == 0.0 { 0.0 } else { f };
        if f.is_nan() { Err(OrderedFloatError) } else { Ok(OrderedF32(normalized)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::convert::TryFrom;

    #[test]
    fn test_sorting() {
        let mut values = vec![
            OrderedF32::try_from(10.0).unwrap(),
            OrderedF32::try_from(2.0).unwrap(),
            OrderedF32::try_from(5.0).unwrap(),
        ];
        values.sort();
        let sorted: Vec<f32> = values.into_iter().map(|v| v.0).collect();
        assert_eq!(sorted, vec![2.0, 5.0, 10.0]);
    }

    #[test]
    fn test_hash_eq() {
        let a = OrderedF32::try_from(1.0).unwrap();
        let b = OrderedF32::try_from(1.0).unwrap();

        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn test_normalizes_zero() {
        let pos_zero = OrderedF32::try_from(0.0).unwrap();
        let neg_zero = OrderedF32::try_from(-0.0).unwrap();

        assert_eq!(pos_zero, neg_zero);

        let mut set = HashSet::new();
        set.insert(pos_zero);
        assert!(set.contains(&neg_zero));
    }

    #[test]
    fn test_nan_fails() {
        assert!(OrderedF32::try_from(f32::NAN).is_err());
    }
}
