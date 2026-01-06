// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod lifecycle;

pub use lifecycle::*;
use reifydb_core::{event::lifecycle::OnCreateEvent, interface::WithEventBus as _};
use reifydb_engine::StandardEngine;

pub trait WithEventBus {
	fn engine(&self) -> &StandardEngine;

	fn on_create<F>(self, f: F) -> Self
	where
		Self: Sized,
		F: Fn(OnCreateContext) + Send + Sync + 'static,
	{
		let callback = OnCreateEventListener::new(self.engine().clone(), f);

		self.engine().event_bus().register::<OnCreateEvent, _>(callback);
		self
	}
}
