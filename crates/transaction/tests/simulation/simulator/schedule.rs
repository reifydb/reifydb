// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Identifies a transaction within a schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxId(pub u32);

/// An operation that can be executed within a transaction.
#[derive(Debug, Clone)]
pub enum Op {
	BeginCommand,
	BeginQuery,
	Set {
		key: String,
		value: String,
	},
	Get {
		key: String,
	},
	Remove {
		key: String,
	},
	Scan,
	Commit,
	Rollback,
}

/// A single step in a schedule: one operation on one transaction.
#[derive(Debug, Clone)]
pub struct Step {
	pub tx_id: TxId,
	pub op: Op,
}

/// An ordered sequence of steps to execute deterministically.
#[derive(Debug, Clone)]
pub struct Schedule {
	pub steps: Vec<Step>,
}

impl Schedule {
	pub fn builder() -> ScheduleBuilder {
		ScheduleBuilder {
			steps: Vec::new(),
		}
	}
}

pub struct ScheduleBuilder {
	steps: Vec<Step>,
}

impl ScheduleBuilder {
	pub fn begin(mut self, tx: u32) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::BeginCommand,
		});
		self
	}

	pub fn begin_query(mut self, tx: u32) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::BeginQuery,
		});
		self
	}

	pub fn set(mut self, tx: u32, key: &str, value: &str) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::Set {
				key: key.to_string(),
				value: value.to_string(),
			},
		});
		self
	}

	pub fn get(mut self, tx: u32, key: &str) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::Get {
				key: key.to_string(),
			},
		});
		self
	}

	pub fn remove(mut self, tx: u32, key: &str) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::Remove {
				key: key.to_string(),
			},
		});
		self
	}

	pub fn scan(mut self, tx: u32) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::Scan,
		});
		self
	}

	pub fn commit(mut self, tx: u32) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::Commit,
		});
		self
	}

	pub fn rollback(mut self, tx: u32) -> Self {
		self.steps.push(Step {
			tx_id: TxId(tx),
			op: Op::Rollback,
		});
		self
	}

	pub fn build(self) -> Schedule {
		Schedule {
			steps: self.steps,
		}
	}
}
