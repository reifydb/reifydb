// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::impl_hook;

pub struct OnStartHook {}

impl_hook!(OnStartHook);

pub struct OnCreateHook {}

impl_hook!(OnCreateHook);
