// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{GetHooks, Unversioned};
use reifydb_core::delta::Delta;
use reifydb_core::{AsyncCowVec, EncodedKey};

pub trait UnversionedStorage:
    Send + Sync + Clone + GetHooks + UnversionedApply + UnversionedGet
{
}

pub trait UnversionedApply {
    fn apply(&self, delta: AsyncCowVec<Delta>);
}

pub trait UnversionedGet {
    fn get(&self, key: &EncodedKey) -> Option<Unversioned>;
}
