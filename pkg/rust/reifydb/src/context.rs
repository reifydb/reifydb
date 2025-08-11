// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! System Context and Runtime Management
//!
//! This module provides the SystemContext trait and associated types for managing
//! shared resources across subsystems using static dispatch. The context acts as
//! an IoC (Inversion of Control) container that provides access to shared resources
//! like async runtimes without requiring dynamic dispatch.

use std::future::Future;
use std::sync::Arc;
#[cfg(feature = "async")]
use tokio::task::JoinHandle;

/// Runtime provider enum for different async runtime implementations
///
/// Uses an enum instead of trait objects to maintain static dispatch
/// and avoid object safety issues.
#[derive(Debug, Clone)]
pub enum RuntimeProvider {
    /// No runtime available (for sync contexts)
    None(NoRuntimeProvider),
    /// Tokio runtime provider
    #[cfg(feature = "async")]
    Tokio(TokioRuntimeProvider),
}

impl RuntimeProvider {
    /// Spawn a future on the runtime
    #[cfg(feature = "async")]
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        match self {
            RuntimeProvider::None(provider) => provider.spawn(future),
            #[cfg(feature = "async")]
            RuntimeProvider::Tokio(provider) => provider.spawn(future),
        }
    }

    /// Block on a future until completion
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        match self {
            RuntimeProvider::None(provider) => provider.block_on(future),
            #[cfg(feature = "async")]
            RuntimeProvider::Tokio(provider) => provider.block_on(future),
        }
    }

    /// Get a handle to the runtime for direct tokio operations
    #[cfg(feature = "async")]
    pub fn handle(&self) -> &tokio::runtime::Handle {
        match self {
            RuntimeProvider::None(provider) => provider.handle(),
            #[cfg(feature = "async")]
            RuntimeProvider::Tokio(provider) => provider.handle(),
        }
    }
}

/// System context trait providing access to shared resources
///
/// Uses the RuntimeProvider enum for consistent interface across all contexts.
pub trait SystemContext: Send + Sync + 'static {
    /// Get access to the runtime provider
    fn runtime(&self) -> &RuntimeProvider;

    /// Check if this context supports async operations
    fn supports_async(&self) -> bool;
}

/// No-op runtime provider for synchronous contexts
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

/// Async context with a shared runtime
///
/// This context provides access to a shared async runtime that can be used
/// by multiple subsystems to avoid creating individual runtimes.
#[derive(Debug, Clone)]
pub struct AsyncContext {
    runtime_provider: RuntimeProvider,
}

impl AsyncContext {
    /// Create a new async context with the given runtime provider
    pub fn new(runtime_provider: RuntimeProvider) -> Self {
        Self { runtime_provider }
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

/// Built-in Tokio runtime provider
///
/// This provides a default tokio runtime implementation that can be shared
/// across subsystems.
#[cfg(feature = "async")]
#[derive(Debug, Clone)]
pub struct TokioRuntimeProvider {
    runtime: Arc<tokio::runtime::Runtime>,
}

#[cfg(feature = "async")]
impl TokioRuntimeProvider {
    /// Create a new Tokio runtime provider with default configuration
    pub fn new() -> Result<Self, tokio::io::Error> {
        let runtime = Arc::new(tokio::runtime::Builder::new_multi_thread().enable_all().build()?);
        Ok(Self { runtime })
    }

    /// Create a new Tokio runtime provider with custom configuration
    pub fn with_builder(mut builder: tokio::runtime::Builder) -> Result<Self, tokio::io::Error> {
        let runtime = Arc::new(builder.build()?);
        Ok(Self { runtime })
    }

    /// Create a new Tokio runtime provider from an existing runtime
    pub fn from_runtime(runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { runtime }
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

/// Convenience type for async context with Tokio runtime
pub type TokioContext = AsyncContext;

#[cfg(feature = "async")]
impl TokioContext {
    /// Create a new Tokio context with default runtime configuration
    pub fn default() -> Result<Self, tokio::io::Error> {
        let provider = TokioRuntimeProvider::new()?;
        Ok(AsyncContext::new(RuntimeProvider::Tokio(provider)))
    }

    /// Create a new Tokio context with custom runtime configuration
    pub fn with_builder(builder: tokio::runtime::Builder) -> Result<Self, tokio::io::Error> {
        let provider = TokioRuntimeProvider::with_builder(builder)?;
        Ok(AsyncContext::new(RuntimeProvider::Tokio(provider)))
    }

    /// Create a new Tokio context from an existing runtime
    #[cfg(feature = "async")]
    pub fn from_runtime(runtime: Arc<tokio::runtime::Runtime>) -> Self {
        let provider = TokioRuntimeProvider::from_runtime(runtime);
        AsyncContext::new(RuntimeProvider::Tokio(provider))
    }
}
