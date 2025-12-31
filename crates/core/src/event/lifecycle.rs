// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::impl_event;

/// Invoked during application startup before database initialization
#[derive(Clone)]
pub struct OnStartEvent {}

impl_event!(OnStartEvent);

/// Invoked once during database creation to setup the internal database system
#[derive(Clone)]
pub struct OnCreateEvent {}

impl_event!(OnCreateEvent);
