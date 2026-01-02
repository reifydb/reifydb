// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML operation compilation.

use crate::{
	bytecode::{
		compile::{PlanCompiler, Result},
		opcode::Opcode,
		program::{DmlTarget, DmlTargetType},
	},
	plan::node::mutate::{DeleteNode, DeleteTarget, InsertNode, InsertTarget, UpdateNode, UpdateTarget},
};

impl PlanCompiler {
	pub(crate) fn compile_insert<'bump>(&mut self, node: &InsertNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile input data
		self.compile_plan(node.input)?;

		// Add DML target
		let target = DmlTarget {
			target_type: match &node.target {
				InsertTarget::Table(_) => DmlTargetType::Table,
				InsertTarget::RingBuffer(_) => DmlTargetType::RingBuffer,
				InsertTarget::Dictionary(_) => DmlTargetType::Dictionary,
			},
			name: match &node.target {
				InsertTarget::Table(t) => t.name.to_string(),
				InsertTarget::RingBuffer(rb) => rb.name.to_string(),
				InsertTarget::Dictionary(d) => d.name.to_string(),
			},
			columns: node.columns.map(|cols| cols.iter().map(|c| c.name.to_string()).collect()),
		};

		let target_index = self.program.add_dml_target(target);
		self.writer.emit_opcode(Opcode::InsertRow);
		self.writer.emit_u16(target_index);
		Ok(())
	}

	pub(crate) fn compile_update<'bump>(&mut self, node: &UpdateNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		if let Some(input) = node.input {
			self.compile_plan(input)?;
		}

		let target = DmlTarget {
			target_type: match &node.target {
				UpdateTarget::Table(_) => DmlTargetType::Table,
				UpdateTarget::RingBuffer(_) => DmlTargetType::RingBuffer,
			},
			name: match &node.target {
				UpdateTarget::Table(t) => t.name.to_string(),
				UpdateTarget::RingBuffer(rb) => rb.name.to_string(),
			},
			columns: None,
		};

		let target_index = self.program.add_dml_target(target);
		self.writer.emit_opcode(Opcode::UpdateRow);
		self.writer.emit_u16(target_index);
		Ok(())
	}

	pub(crate) fn compile_delete<'bump>(&mut self, node: &DeleteNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		if let Some(input) = node.input {
			self.compile_plan(input)?;
		}

		let target = DmlTarget {
			target_type: match &node.target {
				DeleteTarget::Table(_) => DmlTargetType::Table,
				DeleteTarget::RingBuffer(_) => DmlTargetType::RingBuffer,
			},
			name: match &node.target {
				DeleteTarget::Table(t) => t.name.to_string(),
				DeleteTarget::RingBuffer(rb) => rb.name.to_string(),
			},
			columns: None,
		};

		let target_index = self.program.add_dml_target(target);
		self.writer.emit_opcode(Opcode::DeleteRow);
		self.writer.emit_u16(target_index);
		Ok(())
	}
}
