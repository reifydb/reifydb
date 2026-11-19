// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::state::timer::TimerStore;

use crate::operator::state::forget_pass::IdentityForgetPass;

pub trait WindowStore: IdentityForgetPass + TimerStore {}

impl<T: IdentityForgetPass + TimerStore + ?Sized> WindowStore for T {}
