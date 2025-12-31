// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[derive(Debug, Clone)]
pub enum RuntimeProvider {
	None(NoRuntimeProvider),
}

impl RuntimeProvider {
	pub fn new() -> Self {
		RuntimeProvider::None(NoRuntimeProvider)
	}
}

impl Default for RuntimeProvider {
	fn default() -> Self {
		Self::new()
	}
}

pub trait SystemContext: Send + Sync + 'static {
	fn runtime(&self) -> &RuntimeProvider;
	fn supports_async(&self) -> bool {
		false
	}
}

#[derive(Debug, Clone)]
pub struct NoRuntimeProvider;

#[derive(Debug, Clone)]
pub struct SyncContext {
	runtime_provider: RuntimeProvider,
}

impl SyncContext {
	pub fn new() -> Self {
		Self {
			runtime_provider: RuntimeProvider::None(NoRuntimeProvider),
		}
	}
}

impl Default for SyncContext {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemContext for SyncContext {
	fn runtime(&self) -> &RuntimeProvider {
		&self.runtime_provider
	}

	fn supports_async(&self) -> bool {
		false
	}
}
