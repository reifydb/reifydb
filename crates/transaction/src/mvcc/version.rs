// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use base::encoding;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};

/// An MVCC version represents a logical timestamp. Each version belongs to a
/// separate read/write transaction. The latest version is incremented when a
/// new read-write transaction begins.
#[derive(Copy, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
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

impl encoding::Value for Version {}
