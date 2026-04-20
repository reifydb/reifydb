// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Request-level interceptors for pre/post query execution hooks.
//!
//! This module provides an async interceptor mechanism that allows consumers
//! to hook into the request lifecycle — before and after query execution.
//! Interceptors can reject requests (for auth, rate limiting, credit checks)
//! or observe results (for logging, billing, usage tracking).
//!
//! # Example
//!
//! ```ignore
//! use reifydb::server;
//!
//! struct MyInterceptor;
//!
//! impl RequestInterceptor for MyInterceptor {
//!     fn pre_execute(&self, ctx: &mut RequestContext)
//!         -> Pin<Box<dyn Future<Output = Result<(), ExecuteError>> + Send + '_>>
//!     {
//!         Box::pin(async move {
//!             if ctx.metadata.get("authorization").is_none() {
//!                 return Err(ExecuteError::Rejected {
//!                     code: "AUTH_REQUIRED".into(),
//!                     message: "Missing API key".into(),
//!                 });
//!             }
//!             Ok(())
//!         })
//!     }
//!
//!     fn post_execute(&self, ctx: &ResponseContext)
//!         -> Pin<Box<dyn Future<Output = ()> + Send + '_>>
//!     {
//!         Box::pin(async move {
//!             tracing::info!("query executed: {:?}", ctx.metrics.total);
//!         })
//!     }
//! }
//!
//! let db = server::memory()
//!     .with_request_interceptor(MyInterceptor)
//!     .build()?;
//! ```

use std::{collections::HashMap, future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

use futures_util::FutureExt;
use reifydb_core::{actors::server::Operation, metric::ExecutionMetrics};
use reifydb_type::{params::Params, value::identity::IdentityId};
use tracing::error;

use crate::execute::ExecuteError;

/// The transport protocol used for the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
	#[default]
	Http,
	WebSocket,
	Grpc,
}

/// Protocol-agnostic metadata extracted from the request transport layer.
///
/// HTTP headers, gRPC metadata, and WS auth tokens are all normalized into
/// a string-keyed map. Header names are lowercased for consistent lookup.
///
/// Note: this is a single-value map — duplicate keys are overwritten
/// (last-write-wins). Multi-valued headers (e.g. `Set-Cookie`) only
/// retain the last value. This is intentional for simplicity; most
/// interceptor use cases only need single-valued lookups.
#[derive(Debug, Clone, Default)]
pub struct RequestMetadata {
	headers: HashMap<String, String>,
	protocol: Protocol,
}

impl RequestMetadata {
	/// Create empty metadata for the given protocol.
	pub fn new(protocol: Protocol) -> Self {
		Self {
			headers: HashMap::new(),
			protocol,
		}
	}

	/// Insert a header (key is lowercased). Duplicate keys are overwritten.
	pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
		self.headers.insert(key.into().to_ascii_lowercase(), value.into());
	}

	/// Get a header value by name (case-insensitive).
	pub fn get(&self, key: &str) -> Option<&str> {
		self.headers.get(&key.to_ascii_lowercase()).map(|s| s.as_str())
	}

	/// Get the protocol.
	pub fn protocol(&self) -> Protocol {
		self.protocol
	}

	/// Get all headers.
	pub fn headers(&self) -> &HashMap<String, String> {
		&self.headers
	}
}

/// Context available to pre-execute interceptors.
///
/// Fields are public and mutable so interceptors can override values
/// (e.g., resolve API key → set identity, store key_id in metadata for post_execute).
pub struct RequestContext {
	/// The resolved identity. Pre-execute interceptors may replace this.
	pub identity: IdentityId,
	/// The operation type.
	pub operation: Operation,
	/// The RQL string being executed.
	pub rql: String,
	/// Query parameters.
	pub params: Params,
	/// Protocol-agnostic request metadata (headers, etc.).
	pub metadata: RequestMetadata,
}

/// Context available to post-execute interceptors.
pub struct ResponseContext {
	/// The identity that executed the request (may have been mutated by pre_execute).
	pub identity: IdentityId,
	/// The operation type.
	pub operation: Operation,
	/// The RQL string that was executed.
	pub rql: String,
	/// Rich metrics for each statement in the request.
	pub metrics: ExecutionMetrics,
	/// Query parameters.
	pub params: Params,
	/// Protocol-agnostic request metadata.
	pub metadata: RequestMetadata,
	/// Execution result: Ok(frame_count) or Err with the error message.
	pub result: Result<usize, String>,
}

/// Async trait for request-level interceptors.
///
/// Interceptors run in the tokio async context (before compute pool dispatch),
/// so they can perform async I/O (database lookups, network calls, etc.).
///
/// Multiple interceptors are chained: `pre_execute` runs in registration order,
/// `post_execute` runs in reverse order (like middleware stacks).
pub trait RequestInterceptor: Send + Sync + 'static {
	/// Called before query execution.
	///
	/// Return `Ok(())` to allow the request to proceed.
	/// Return `Err(ExecuteError)` to reject the request.
	/// May mutate the context (e.g., set identity from API key lookup).
	fn pre_execute<'a>(
		&'a self,
		ctx: &'a mut RequestContext,
	) -> Pin<Box<dyn Future<Output = Result<(), ExecuteError>> + Send + 'a>>;

	/// Called after query execution completes (success or failure).
	///
	/// This is called even if the execution failed, so interceptors can
	/// log failures and track usage regardless of outcome.
	fn post_execute<'a>(&'a self, ctx: &'a ResponseContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Ordered chain of request interceptors, cheap to clone (Arc internally).
#[derive(Clone)]
pub struct RequestInterceptorChain {
	interceptors: Arc<Vec<Arc<dyn RequestInterceptor>>>,
}

impl RequestInterceptorChain {
	pub fn new(interceptors: Vec<Arc<dyn RequestInterceptor>>) -> Self {
		Self {
			interceptors: Arc::new(interceptors),
		}
	}

	pub fn empty() -> Self {
		Self {
			interceptors: Arc::new(Vec::new()),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.interceptors.is_empty()
	}

	/// Run all pre_execute interceptors in order.
	/// Stops and returns Err on first rejection.
	pub async fn pre_execute(&self, ctx: &mut RequestContext) -> Result<(), ExecuteError> {
		for interceptor in self.interceptors.iter() {
			interceptor.pre_execute(ctx).await?;
		}
		Ok(())
	}

	/// Run all post_execute interceptors in reverse order.
	///
	/// If an interceptor panics, the panic is caught and logged so that
	/// remaining interceptors still run.
	pub async fn post_execute(&self, ctx: &ResponseContext) {
		for interceptor in self.interceptors.iter().rev() {
			if let Err(panic) = AssertUnwindSafe(interceptor.post_execute(ctx)).catch_unwind().await {
				let msg = panic
					.downcast_ref::<&str>()
					.copied()
					.or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
					.unwrap_or("unknown panic");
				error!("post_execute interceptor panicked: {}", msg);
			}
		}
	}
}

impl Default for RequestInterceptorChain {
	fn default() -> Self {
		Self::empty()
	}
}
