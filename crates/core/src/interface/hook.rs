// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::Hooks;

pub trait GetHooks {
    fn hooks(&self) -> Hooks;
}
