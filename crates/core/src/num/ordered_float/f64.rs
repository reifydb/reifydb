// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::num::ordered_float::error::OrderedFloatError;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, Default)]
pub struct OrderedF64(f64);

impl Serialize for OrderedF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> Deserialize<'de> for OrderedF64 {
    fn deserialize<D>(deserializer: D) -> Result<OrderedF64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct F64Visitor;

        impl Visitor<'_> for F64Visitor {
            type Value = OrderedF64;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a 64-bit floating point number")
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(OrderedF64(value))
            }

            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OrderedF64(value as f64))
            }
        }

        deserializer.deserialize_f64(F64Visitor)
    }
}

impl OrderedF64 {
    pub fn value(&self) -> f64 {
        self.0
    }

    pub fn zero() -> OrderedF64 {
        OrderedF64(0.0f64)
    }
}

impl Deref for OrderedF64 {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for OrderedF64 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        let l = self.0.to_bits() ^ ((self.0.to_bits() >> 63) & 0x7fffffffffffffff);
        let r = other.0.to_bits() ^ ((other.0.to_bits() >> 63) & 0x7fffffffffffffff);
        l.cmp(&r)
    }
}

impl Hash for OrderedF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl From<OrderedF64> for f64 {
    fn from(v: OrderedF64) -> Self {
        v.0
    }
}

impl TryFrom<f64> for OrderedF64 {
    type Error = OrderedFloatError;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        // normalize -0.0 and +0.0
        let normalized = if f == 0.0 { 0.0 } else { f };
        if f.is_nan() { Err(OrderedFloatError) } else { Ok(OrderedF64(normalized)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::convert::TryFrom;

    #[test]
    fn test_eq_and_ord() {
        let a = OrderedF64::try_from(3.14).unwrap();
        let b = OrderedF64::try_from(3.14).unwrap();
        let c = OrderedF64::try_from(2.71).unwrap();

        assert_eq!(a, b);
        assert!(a > c);
        assert!(c < a);
    }

    #[test]
    fn test_sorting() {
        let mut values = vec![
            OrderedF64::try_from(10.0).unwrap(),
            OrderedF64::try_from(2.0).unwrap(),
            OrderedF64::try_from(5.0).unwrap(),
        ];
        values.sort();
        let sorted: Vec<f64> = values.into_iter().map(|v| v.0).collect();
        assert_eq!(sorted, vec![2.0, 5.0, 10.0]);
    }

    #[test]
    fn test_hash_eq() {
        let a = OrderedF64::try_from(1.0).unwrap();
        let b = OrderedF64::try_from(1.0).unwrap();

        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn test_normalizes_zero() {
        let pos_zero = OrderedF64::try_from(0.0).unwrap();
        let neg_zero = OrderedF64::try_from(-0.0).unwrap();

        assert_eq!(pos_zero, neg_zero);

        let mut set = HashSet::new();
        set.insert(pos_zero);
        assert!(set.contains(&neg_zero));
    }

    #[test]
    fn test_nan_fails() {
        assert!(OrderedF64::try_from(f64::NAN).is_err());
    }
}
