// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::state::timer::TimerStore;

use crate::operator::state::reaper::IdentityReclaim;

pub trait WindowStore: IdentityReclaim + TimerStore {}

impl<T: IdentityReclaim + TimerStore + ?Sized> WindowStore for T {}
