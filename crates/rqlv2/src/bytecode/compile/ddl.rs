// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL operation compilation.

use crate::{
	bytecode::{
		compile::{CompileError, PlanCompiler, Result},
		opcode::{ObjectType, Opcode},
		program::{
			ColumnDef, CreateDictionaryDef, CreateIndexDef, CreateNamespaceDef, CreateRingBufferDef,
			CreateSequenceDef, CreateTableDef, CreateViewDef, DdlDef, DropDef,
		},
	},
	plan::node::ddl::{AlterNode, CreateNode, DropNode, DropTarget},
};

impl PlanCompiler {
	pub(crate) fn compile_create<'bump>(&mut self, node: &CreateNode<'bump>) -> Result<()> {
		match node {
			CreateNode::Namespace(n) => {
				self.record_span(n.span);
				let def = DdlDef::CreateNamespace(CreateNamespaceDef {
					name: n.name.to_string(),
					if_not_exists: n.if_not_exists,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateNamespace);
				self.writer.emit_u16(index);
			}
			CreateNode::Table(t) => {
				self.record_span(t.span);
				let def = DdlDef::CreateTable(CreateTableDef {
					namespace: Some(t.namespace.name.to_string()),
					name: t.name.to_string(),
					columns: t
						.columns
						.iter()
						.map(|c| ColumnDef {
							name: c.name.to_string(),
							data_type: format!("{:?}", c.column_type),
							nullable: c.nullable,
							default: None, // TODO: Handle defaults
						})
						.collect(),
					primary_key: t
						.primary_key
						.map(|pk| pk.iter().map(|c| (*c).to_string()).collect()),
					if_not_exists: t.if_not_exists,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateTable);
				self.writer.emit_u16(index);
			}
			CreateNode::View(v) => {
				self.record_span(v.span);
				// Compile the view's query first
				let query_offset = self.writer.position();
				self.compile_plan(v.query)?;

				let def = DdlDef::CreateView(CreateViewDef {
					namespace: Some(v.namespace.name.to_string()),
					name: v.name.to_string(),
					query_bytecode_offset: query_offset,
					if_not_exists: v.if_not_exists,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateView);
				self.writer.emit_u16(index);
			}
			CreateNode::Index(i) => {
				self.record_span(i.span);
				let def = DdlDef::CreateIndex(CreateIndexDef {
					table: i.table.name.to_string(),
					name: i.name.to_string(),
					columns: i.columns.iter().map(|c| c.column.name.to_string()).collect(),
					unique: i.unique,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateIndex);
				self.writer.emit_u16(index);
			}
			CreateNode::Sequence(s) => {
				self.record_span(s.span);
				let def = DdlDef::CreateSequence(CreateSequenceDef {
					namespace: Some(s.namespace.name.to_string()),
					name: s.name.to_string(),
					start: s.start.unwrap_or(1),
					increment: s.increment.unwrap_or(1),
					if_not_exists: s.if_not_exists,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateSequence);
				self.writer.emit_u16(index);
			}
			CreateNode::RingBuffer(rb) => {
				self.record_span(rb.span);
				let def = DdlDef::CreateRingBuffer(CreateRingBufferDef {
					namespace: Some(rb.namespace.name.to_string()),
					name: rb.name.to_string(),
					columns: rb
						.columns
						.iter()
						.map(|c| ColumnDef {
							name: c.name.to_string(),
							data_type: format!("{:?}", c.column_type),
							nullable: c.nullable,
							default: None,
						})
						.collect(),
					capacity: rb.capacity,
					if_not_exists: rb.if_not_exists,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateRingBuffer);
				self.writer.emit_u16(index);
			}
			CreateNode::Dictionary(d) => {
				self.record_span(d.span);
				let def = DdlDef::CreateDictionary(CreateDictionaryDef {
					namespace: Some(d.namespace.name.to_string()),
					name: d.name.to_string(),
					key_type: format!("{:?}", d.key_type),
					value_type: format!("{:?}", d.value_type),
					if_not_exists: d.if_not_exists,
				});
				let index = self.builder.add_ddl_def(def);
				self.writer.emit_opcode(Opcode::CreateDictionary);
				self.writer.emit_u16(index);
			}
		}
		Ok(())
	}

	pub(crate) fn compile_alter<'bump>(&mut self, node: &AlterNode<'bump>) -> Result<()> {
		match node {
			AlterNode::Table(t) => {
				self.record_span(t.span);
				// TODO: Implement alter table
				Err(CompileError::UnsupportedPlan {
					message: "alter table not yet supported".to_string(),
					span: t.span,
				})
			}
			AlterNode::Sequence(s) => {
				self.record_span(s.span);
				Err(CompileError::UnsupportedPlan {
					message: "alter sequence not yet supported".to_string(),
					span: s.span,
				})
			}
		}
	}

	pub(crate) fn compile_drop<'bump>(&mut self, node: &DropNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		let (object_type, name) = match &node.target {
			DropTarget::Namespace(n) => (ObjectType::Namespace, (*n).to_string()),
			DropTarget::Table(t) => (ObjectType::Table, t.name.to_string()),
			DropTarget::View(v) => (ObjectType::View, v.name.to_string()),
			DropTarget::Index(i) => (ObjectType::Index, i.name.to_string()),
			DropTarget::Sequence(s) => (ObjectType::Sequence, s.name.to_string()),
			DropTarget::RingBuffer(rb) => (ObjectType::RingBuffer, rb.name.to_string()),
			DropTarget::Dictionary(d) => (ObjectType::Dictionary, d.name.to_string()),
		};

		let def = DdlDef::Drop(DropDef {
			object_type,
			name,
			if_exists: node.if_exists,
		});

		let index = self.builder.add_ddl_def(def);
		self.writer.emit_opcode(Opcode::DropObject);
		self.writer.emit_u16(index);
		self.writer.emit_u8(object_type as u8);
		Ok(())
	}
}
