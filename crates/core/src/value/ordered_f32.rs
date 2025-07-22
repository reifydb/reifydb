// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{err, Error};
use crate::error::diagnostic::number::nan_not_allowed;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// A wrapper around f32 that provides total ordering by rejecting NaN values.
/// This type is sortable and can be used in collections that require Ord,
/// such as BTreeMap and BTreeSet. It prevents NaN values from being stored,
/// ensuring that all values are comparable and can be sorted consistently.
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Default)]
pub struct OrderedF32(f32);

impl Serialize for OrderedF32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f32(self.0)
    }
}

impl<'de> Deserialize<'de> for OrderedF32 {
    fn deserialize<D>(deserializer: D) -> Result<OrderedF32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct F32Visitor;

        impl Visitor<'_> for F32Visitor {
            type Value = OrderedF32;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a 32-bit floating point number")
            }

            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
                Ok(OrderedF32(value))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OrderedF32(value as f32))
            }
        }

        deserializer.deserialize_f32(F32Visitor)
    }
}

impl OrderedF32 {
    pub fn value(&self) -> f32 {
        self.0
    }

    pub fn zero() -> OrderedF32 {
        OrderedF32(0.0f32)
    }
}

impl Deref for OrderedF32 {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for OrderedF32 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl PartialEq for OrderedF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedF32 {}

impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        let l = self.0.to_bits() ^ ((self.0.to_bits() >> 31) & 0x7fffffff);
        let r = other.0.to_bits() ^ ((other.0.to_bits() >> 31) & 0x7fffffff);
        l.cmp(&r)
    }
}

impl Hash for OrderedF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl From<OrderedF32> for f32 {
    fn from(v: OrderedF32) -> Self {
        v.0
    }
}

impl TryFrom<f32> for OrderedF32 {
    type Error = Error;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        let normalized = if f == 0.0 { 0.0 } else { f };
        if f.is_nan() { err!(nan_not_allowed()) } else { Ok(OrderedF32(normalized)) }
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