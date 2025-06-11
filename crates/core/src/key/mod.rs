// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use range::EncodedKeyRange;

mod range;

use crate::AsyncCowVec;

pub type EncodedKey = AsyncCowVec<u8>;
