// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod lifecycle;

pub use lifecycle::*;
use reifydb_core::{
	event::lifecycle::OnCreateEvent,
	interface::{Transaction, WithEventBus as _},
};
use reifydb_engine::StandardEngine;

pub trait WithEventBus<T: Transaction> {
	fn engine(&self) -> &StandardEngine<T>;

	fn on_create<F>(self, f: F) -> Self
	where
		Self: Sized,
		F: Fn(&OnCreateContext<T>) -> crate::Result<()>
			+ Send
			+ Sync
			+ 'static,
	{
		let callback = OnCreateEventListener {
			callback: f,
			engine: self.engine().clone(),
		};

		self.engine()
			.event_bus()
			.register::<OnCreateEvent, _>(callback);
		self
	}
}
