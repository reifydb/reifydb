// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::impl_hook;

/// Invoked during application startup before database initialization
pub struct OnStartHook {}

impl_hook!(OnStartHook);

/// Invoked once during database creation to setup the internal database system
pub struct OnCreateHook {}

impl_hook!(OnCreateHook);
