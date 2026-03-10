// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use crate::{
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::catalog::column::ColumnDef,
	value::column::columns::Columns,
};

/// A captured event dispatch during test execution.
#[derive(Clone, Debug)]
pub struct CapturedEvent {
	pub sequence: u64,
	pub namespace: String,
	pub event: String,
	pub variant: String,
	pub depth: u8,
	pub columns: Columns,
}

/// A captured handler invocation during test execution.
#[derive(Clone, Debug)]
pub struct HandlerInvocation {
	pub sequence: u64,
	pub namespace: String,
	pub handler: String,
	pub event: String,
	pub variant: String,
	pub duration_ns: u64,
	pub outcome: String,
	pub message: String,
}

/// A captured mutation (insert/update/delete) during test execution.
#[derive(Clone, Debug)]
pub struct MutationRecord {
	pub sequence: u64,
	pub op: String,
	pub old: Columns,
	pub new: Columns,
}

/// Audit log that captures events, handler invocations, and mutations during test execution.
///
/// Only allocated when `vm.in_test_context` is true. Zero cost in production.
#[derive(Clone, Debug)]
pub struct TestingContext {
	pub events: Vec<CapturedEvent>,
	pub handler_invocations: Vec<HandlerInvocation>,
	/// Keyed by "namespace::primitive_name"
	pub mutations: HashMap<String, Vec<MutationRecord>>,
	event_seq: u64,
	handler_seq: u64,
	mutation_seq: u64,
}

impl TestingContext {
	pub fn new() -> Self {
		Self {
			events: Vec::new(),
			handler_invocations: Vec::new(),
			mutations: HashMap::new(),
			event_seq: 0,
			handler_seq: 0,
			mutation_seq: 0,
		}
	}

	pub fn clear(&mut self) {
		self.events.clear();
		self.handler_invocations.clear();
		self.mutations.clear();
		self.event_seq = 0;
		self.handler_seq = 0;
		self.mutation_seq = 0;
	}

	pub fn record_event(&mut self, namespace: String, event: String, variant: String, depth: u8, columns: Columns) {
		self.event_seq += 1;
		self.events.push(CapturedEvent {
			sequence: self.event_seq,
			namespace,
			event,
			variant,
			depth,
			columns,
		});
	}

	pub fn record_handler_invocation(
		&mut self,
		namespace: String,
		handler: String,
		event: String,
		variant: String,
		duration_ns: u64,
		outcome: String,
		message: String,
	) {
		self.handler_seq += 1;
		self.handler_invocations.push(HandlerInvocation {
			sequence: self.handler_seq,
			namespace,
			handler,
			event,
			variant,
			duration_ns,
			outcome,
			message,
		});
	}

	pub fn record_mutation(&mut self, primitive_key: String, op: String, old: Columns, new: Columns) {
		self.mutation_seq += 1;
		self.mutations.entry(primitive_key).or_default().push(MutationRecord {
			sequence: self.mutation_seq,
			op,
			old,
			new,
		});
	}

	pub fn record_insert(&mut self, key: String, new: Columns) {
		self.record_mutation(key, "insert".to_string(), Columns::empty(), new);
	}

	pub fn record_delete(&mut self, key: String, old: Columns) {
		self.record_mutation(key, "delete".to_string(), old, Columns::empty());
	}

	pub fn record_update(&mut self, key: String, old: Columns, new: Columns) {
		self.record_mutation(key, "update".to_string(), old, new);
	}
}

pub fn columns_from_encoded(columns: &[ColumnDef], schema: &Schema, encoded: &EncodedValues) -> Columns {
	Columns::single_row(
		columns.iter().enumerate().map(|(i, col)| (col.name.as_str(), schema.get_value(encoded, i))),
	)
}
