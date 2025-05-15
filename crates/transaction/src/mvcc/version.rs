// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use std::fmt;
use std::fmt::Write;
use base::encoding;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::ops::{Add, Sub};
use serde::de::Visitor;

/// An MVCC version represents a logical timestamp. Each version belongs to a
/// separate read/write transaction. The latest version is incremented when a
/// new read-write transaction begins.
#[derive(Copy, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Version(pub u64);

impl Sub<i32> for Version {
    type Output = Version;

    fn sub(self, rhs: i32) -> Self::Output {
        Version(self.0 - rhs as u64)
    }
}

impl Add<i32> for Version {
    type Output = Version;

    fn add(self, rhs: i32) -> Self::Output {
        Version(self.0 + rhs as u64)
    }
}

// Serialize as a plain u64
impl Serialize for Version {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

// Deserialize from a u64
impl<'de> Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionVisitor;

        impl<'de> Visitor<'de> for VersionVisitor {
            type Value = Version;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u64 representing a Version")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Version, E>
            where
                E: de::Error,
            {
                Ok(Version(value))
            }
        }

        deserializer.deserialize_u64(VersionVisitor)
    }
}


impl encoding::Value for Version {}
