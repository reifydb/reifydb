// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use reifydb_core::interface::catalog::task::TaskId;

use crate::{context::TaskContext, schedule::Schedule};

type SyncTaskFn = Arc<dyn Fn(TaskContext) -> Result<(), Box<dyn Error + Send>> + Send + Sync>;

type AsyncTaskFn = Arc<
	dyn Fn(TaskContext) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send>>> + Send>> + Send + Sync,
>;

#[derive(Clone)]
pub enum TaskWork {
	Sync(SyncTaskFn),

	Async(AsyncTaskFn),
}

impl fmt::Debug for TaskWork {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			TaskWork::Sync(_) => write!(f, "TaskWork::Sync"),
			TaskWork::Async(_) => write!(f, "TaskWork::Async"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutor {
	ComputePool,

	Tokio,
}

pub struct ScheduledTask {
	pub id: TaskId,

	pub name: String,

	pub schedule: Schedule,

	pub work: TaskWork,

	pub executor: TaskExecutor,
}

impl fmt::Debug for ScheduledTask {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ScheduledTask")
			.field("id", &self.id)
			.field("name", &self.name)
			.field("schedule", &self.schedule)
			.field("work", &self.work)
			.field("executor", &self.executor)
			.finish()
	}
}

impl ScheduledTask {
	pub fn builder(name: impl Into<String>) -> ScheduledTaskBuilder {
		ScheduledTaskBuilder::new(name)
	}
}

pub struct ScheduledTaskBuilder {
	name: String,
	schedule: Option<Schedule>,
	work: Option<TaskWork>,
	executor: Option<TaskExecutor>,
}

impl ScheduledTaskBuilder {
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			schedule: None,
			work: None,
			executor: None,
		}
	}

	pub fn schedule(mut self, schedule: Schedule) -> Self {
		self.schedule = Some(schedule);
		self
	}

	pub fn work_sync<F>(mut self, f: F) -> Self
	where
		F: Fn(TaskContext) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
	{
		self.work = Some(TaskWork::Sync(Arc::new(f)));
		self
	}

	pub fn work_async<F, Fut>(mut self, f: F) -> Self
	where
		F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send + 'static,
	{
		self.work = Some(TaskWork::Async(Arc::new(move |ctx| Box::pin(f(ctx)))));
		self
	}

	pub fn executor(mut self, executor: TaskExecutor) -> Self {
		self.executor = Some(executor);
		self
	}

	pub fn build(self) -> Result<ScheduledTask, String> {
		let schedule = self.schedule.ok_or("schedule is required")?;
		let work = self.work.ok_or("work is required")?;
		let executor = self.executor.ok_or("executor is required")?;

		schedule.validate()?;

		Ok(ScheduledTask {
			id: TaskId::new(),
			name: self.name,
			schedule,
			work,
			executor,
		})
	}
}
