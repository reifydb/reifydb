// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use std::panic;
use std::{
	fmt,
	future::Future,
	pin::Pin,
	task::{Context, Poll},
};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use tokio::{runtime::Handle, task::JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskError {
	Shutdown,
}

#[derive(Clone, Debug)]
pub enum Spawner {
	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	Host(Handle),
	Inline,
}

impl Spawner {
	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn host(handle: Handle) -> Self {
		Spawner::Host(handle)
	}

	pub fn inline() -> Self {
		Spawner::Inline
	}

	pub fn spawn<F>(&self, future: F) -> TaskHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		match self {
			#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
			Spawner::Host(handle) => TaskHandle::Host(HostTask {
				inner: handle.spawn(future),
			}),
			Spawner::Inline => TaskHandle::Inline(InlineTask {
				future: Some(Box::pin(future)),
			}),
		}
	}
}

pub enum TaskHandle<T> {
	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	Host(HostTask<T>),
	Inline(InlineTask<T>),
}

impl<T> Future for TaskHandle<T> {
	type Output = Result<T, TaskError>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match self.get_mut() {
			#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
			TaskHandle::Host(task) => task.poll(cx),
			TaskHandle::Inline(task) => task.poll(cx),
		}
	}
}

impl<T> fmt::Debug for TaskHandle<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
			TaskHandle::Host(_) => f.write_str("TaskHandle::Host"),
			TaskHandle::Inline(_) => f.write_str("TaskHandle::Inline"),
		}
	}
}

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub struct HostTask<T> {
	inner: JoinHandle<T>,
}

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
impl<T> HostTask<T> {
	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, TaskError>> {
		match Pin::new(&mut self.inner).poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
			Poll::Ready(Err(error)) if error.is_panic() => panic::resume_unwind(error.into_panic()),
			Poll::Ready(Err(_)) => Poll::Ready(Err(TaskError::Shutdown)),
		}
	}
}

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
impl<T> Drop for HostTask<T> {
	fn drop(&mut self) {
		self.inner.abort();
	}
}

pub struct InlineTask<T> {
	future: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
}

impl<T> InlineTask<T> {
	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, TaskError>> {
		let Some(future) = self.future.as_mut() else {
			return Poll::Ready(Err(TaskError::Shutdown));
		};
		match future.as_mut().poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(value) => {
				self.future = None;
				Poll::Ready(Ok(value))
			}
		}
	}
}
