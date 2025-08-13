// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::future::Future;
#[cfg(feature = "async")]
use std::sync::Arc;

#[cfg(feature = "async")]
use tokio::runtime::Runtime;
#[cfg(feature = "async")]
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub enum RuntimeProvider {
	None(NoRuntimeProvider),
	#[cfg(feature = "async")]
	Tokio(TokioRuntimeProvider),
}

impl RuntimeProvider {
	#[cfg(feature = "async")]
	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send,
	{
		match self {
			RuntimeProvider::None(provider) => {
				provider.spawn(future)
			}
			#[cfg(feature = "async")]
			RuntimeProvider::Tokio(provider) => provider.spawn(future),
		}
	}

	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		match self {
			RuntimeProvider::None(provider) => {
				provider.block_on(future)
			}
			#[cfg(feature = "async")]
			RuntimeProvider::Tokio(provider) => provider.block_on(future),
		}
	}

	#[cfg(feature = "async")]
	pub fn handle(&self) -> &tokio::runtime::Handle {
		match self {
			RuntimeProvider::None(provider) => provider.handle(),
			#[cfg(feature = "async")]
			RuntimeProvider::Tokio(provider) => provider.handle(),
		}
	}
}

pub trait SystemContext: Send + Sync + 'static {
	fn runtime(&self) -> &RuntimeProvider;
	fn supports_async(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct NoRuntimeProvider;

impl NoRuntimeProvider {
	#[cfg(feature = "async")]
	fn spawn<F>(&self, _future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send,
	{
		panic!(
			"Cannot spawn async tasks in synchronous context. Add .with_async_runtime() to enable async operations."
		);
	}

	fn block_on<F>(&self, _future: F) -> F::Output
	where
		F: Future,
	{
		panic!(
			"Cannot block on futures in synchronous context. Add .with_async_runtime() to enable async operations."
		);
	}

	#[cfg(feature = "async")]
	fn handle(&self) -> &tokio::runtime::Handle {
		panic!(
			"No runtime handle available in synchronous context. Add .with_async_runtime() to enable async operations."
		);
	}
}

#[derive(Debug, Clone)]
pub struct AsyncContext {
	runtime_provider: RuntimeProvider,
}

impl AsyncContext {
	/// Create a new async context with the given runtime provider
	pub fn new(runtime_provider: RuntimeProvider) -> Self {
		Self {
			runtime_provider,
		}
	}
}

impl SystemContext for AsyncContext {
	fn runtime(&self) -> &RuntimeProvider {
		&self.runtime_provider
	}

	fn supports_async(&self) -> bool {
		true
	}
}

#[cfg(feature = "async")]
#[derive(Debug, Clone)]
pub struct TokioRuntimeProvider {
	runtime: Arc<Runtime>,
}

#[cfg(feature = "async")]
impl TokioRuntimeProvider {
	pub fn new() -> Result<Self, tokio::io::Error> {
		let runtime = Arc::new(
			tokio::runtime::Builder::new_multi_thread()
				.enable_all()
				.build()?,
		);
		Ok(Self {
			runtime,
		})
	}

	pub fn with_builder(
		mut builder: tokio::runtime::Builder,
	) -> Result<Self, tokio::io::Error> {
		let runtime = Arc::new(builder.build()?);
		Ok(Self {
			runtime,
		})
	}

	pub fn from_runtime(runtime: Arc<Runtime>) -> Self {
		Self {
			runtime,
		}
	}
}

#[cfg(feature = "async")]
impl TokioRuntimeProvider {
	fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send,
	{
		self.runtime.spawn(future)
	}

	fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.runtime.block_on(future)
	}

	fn handle(&self) -> &tokio::runtime::Handle {
		self.runtime.handle()
	}
}

pub type TokioContext = AsyncContext;

#[cfg(feature = "async")]
impl TokioContext {
	pub fn default() -> Result<Self, tokio::io::Error> {
		let provider = TokioRuntimeProvider::new()?;
		Ok(AsyncContext::new(RuntimeProvider::Tokio(provider)))
	}
	pub fn with_builder(
		builder: tokio::runtime::Builder,
	) -> Result<Self, tokio::io::Error> {
		let provider = TokioRuntimeProvider::with_builder(builder)?;
		Ok(AsyncContext::new(RuntimeProvider::Tokio(provider)))
	}
	#[cfg(feature = "async")]
	pub fn from_runtime(runtime: Arc<Runtime>) -> Self {
		let provider = TokioRuntimeProvider::from_runtime(runtime);
		AsyncContext::new(RuntimeProvider::Tokio(provider))
	}
}
