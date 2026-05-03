// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

use futures_util::FutureExt;
use reifydb_core::{actors::server::Operation, metric::ExecutionMetrics};
use reifydb_type::{params::Params, value::identity::IdentityId};
use tracing::error;

use crate::execute::ExecuteError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
	#[default]
	Http,
	WebSocket,
	Grpc,
}

#[derive(Debug, Clone, Default)]
pub struct RequestMetadata {
	headers: HashMap<String, String>,
	protocol: Protocol,
}

impl RequestMetadata {
	pub fn new(protocol: Protocol) -> Self {
		Self {
			headers: HashMap::new(),
			protocol,
		}
	}

	pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
		self.headers.insert(key.into().to_ascii_lowercase(), value.into());
	}

	pub fn get(&self, key: &str) -> Option<&str> {
		self.headers.get(&key.to_ascii_lowercase()).map(|s| s.as_str())
	}

	pub fn protocol(&self) -> Protocol {
		self.protocol
	}

	pub fn headers(&self) -> &HashMap<String, String> {
		&self.headers
	}
}

pub struct RequestContext {
	pub identity: IdentityId,

	pub operation: Operation,

	pub rql: String,

	pub params: Params,

	pub metadata: RequestMetadata,
}

pub struct ResponseContext {
	pub identity: IdentityId,

	pub operation: Operation,

	pub rql: String,

	pub metrics: ExecutionMetrics,

	pub params: Params,

	pub metadata: RequestMetadata,

	pub result: Result<usize, String>,
}

pub trait RequestInterceptor: Send + Sync + 'static {
	fn pre_execute<'a>(
		&'a self,
		ctx: &'a mut RequestContext,
	) -> Pin<Box<dyn Future<Output = Result<(), ExecuteError>> + Send + 'a>>;

	fn post_execute<'a>(&'a self, ctx: &'a ResponseContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

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

	pub async fn pre_execute(&self, ctx: &mut RequestContext) -> Result<(), ExecuteError> {
		for interceptor in self.interceptors.iter() {
			interceptor.pre_execute(ctx).await?;
		}
		Ok(())
	}

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
