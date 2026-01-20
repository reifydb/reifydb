// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::define_event;

define_event! {
	/// Invoked during application startup before database initialization
	pub struct OnStartEvent {}
}

define_event! {
	/// Invoked once during database creation to setup the internal database system
	pub struct OnCreateEvent {}
}
