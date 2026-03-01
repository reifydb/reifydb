use std::{
	error::Error,
	fmt,
	future::Future,
	pin::Pin,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use crate::{context::TaskContext, schedule::Schedule};

/// Unique identifier for a scheduled task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
	/// Generate a new unique task ID
	pub fn new() -> Self {
		static COUNTER: AtomicU64 = AtomicU64::new(1);
		Self(COUNTER.fetch_add(1, Ordering::Relaxed))
	}
}

impl Default for TaskId {
	fn default() -> Self {
		Self::new()
	}
}

impl fmt::Display for TaskId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "task-{}", self.0)
	}
}

/// Defines the type of work a task performs
#[derive(Clone)]
pub enum TaskWork {
	/// Synchronous blocking work (runs on compute pool)
	Sync(Arc<dyn Fn(TaskContext) -> Result<(), Box<dyn Error + Send>> + Send + Sync>),
	/// Asynchronous work (runs on tokio runtime)
	Async(
		Arc<
			dyn Fn(
					TaskContext,
				) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send>>> + Send>>
				+ Send
				+ Sync,
		>,
	),
}

impl fmt::Debug for TaskWork {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			TaskWork::Sync(_) => write!(f, "TaskWork::Sync"),
			TaskWork::Async(_) => write!(f, "TaskWork::Async"),
		}
	}
}

/// Defines where a task should be executed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutor {
	/// Execute on the compute pool (for CPU-bound work)
	ComputePool,
	/// Execute on the tokio runtime (for I/O-bound async work)
	Tokio,
}

/// A scheduled task definition
pub struct ScheduledTask {
	/// Unique identifier for this task
	pub id: TaskId,
	/// Human-readable name
	pub name: String,
	/// When to execute this task
	pub schedule: Schedule,
	/// The work to perform
	pub work: TaskWork,
	/// Where to execute the work
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
	/// Start building a new scheduled task
	pub fn builder(name: impl Into<String>) -> ScheduledTaskBuilder {
		ScheduledTaskBuilder::new(name)
	}
}

/// Builder for creating scheduled tasks
pub struct ScheduledTaskBuilder {
	name: String,
	schedule: Option<Schedule>,
	work: Option<TaskWork>,
	executor: Option<TaskExecutor>,
}

impl ScheduledTaskBuilder {
	/// Create a new task builder
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			schedule: None,
			work: None,
			executor: None,
		}
	}

	/// Set the schedule for this task
	pub fn schedule(mut self, schedule: Schedule) -> Self {
		self.schedule = Some(schedule);
		self
	}

	/// Set synchronous work for this task
	pub fn work_sync<F>(mut self, f: F) -> Self
	where
		F: Fn(TaskContext) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
	{
		self.work = Some(TaskWork::Sync(Arc::new(f)));
		self
	}

	/// Set asynchronous work for this task
	pub fn work_async<F, Fut>(mut self, f: F) -> Self
	where
		F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send + 'static,
	{
		self.work = Some(TaskWork::Async(Arc::new(move |ctx| Box::pin(f(ctx)))));
		self
	}

	/// Set the executor for this task
	pub fn executor(mut self, executor: TaskExecutor) -> Self {
		self.executor = Some(executor);
		self
	}

	/// Build the scheduled task
	pub fn build(self) -> Result<ScheduledTask, String> {
		let schedule = self.schedule.ok_or("schedule is required")?;
		let work = self.work.ok_or("work is required")?;
		let executor = self.executor.ok_or("executor is required")?;

		// Validate the schedule
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
