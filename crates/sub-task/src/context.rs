use reifydb_engine::engine::StandardEngine;

#[derive(Clone)]
pub struct TaskContext {
	engine: StandardEngine,
}

impl TaskContext {
	/// Create a new task context
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}
}
