// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::hook::Hooks;
use crate::interface::{Transaction, UnversionedStorage, VersionedStorage};

pub trait GetHooks<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn hooks(&self) -> Hooks<VS, US, T>;
}
