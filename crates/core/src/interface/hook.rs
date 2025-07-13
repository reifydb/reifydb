// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::hook::Hooks;
use crate::interface::UnversionedStorage;

pub trait GetHooks<US>
where
    US: UnversionedStorage,
{
    fn hooks(&self) -> Hooks<US>;
}
