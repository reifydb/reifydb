// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod lifecycle;

use std::future::Future;

pub use lifecycle::*;
use reifydb_core::{event::lifecycle::OnCreateEvent, interface::WithEventBus as _};
use reifydb_engine::StandardEngine;

#[allow(async_fn_in_trait)]
pub trait WithEventBus {
	fn engine(&self) -> &StandardEngine;

	async fn on_create<F, Fut>(self, f: F) -> Self
	where
		Self: Sized,
		F: Fn(OnCreateContext) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = crate::Result<()>> + Send + 'static,
	{
		let callback = OnCreateEventListener::new(self.engine().clone(), f);

		self.engine().event_bus().register::<OnCreateEvent, _>(callback).await;
		self
	}
}
