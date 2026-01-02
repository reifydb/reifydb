// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query operation compilation.

use crate::{
	bytecode::{
		compile::{CompileError, PlanCompiler, Result},
		opcode::{Opcode, OperatorKind},
		program::{Constant, NullsOrder, SortDirection, SortKey, SortSpec, SourceDef},
	},
	expression::compile::{compile_plan_expr, compile_plan_filter},
	plan::{
		Primitive,
		node::query::{
			AggregateNode, ApplyNode, DistinctNode, ExtendNode, FilterNode, GeneratorNode, IndexScanNode,
			InlineDataNode, JoinInnerNode, JoinLeftNode, JoinNaturalNode, MergeNode, ProjectNode,
			RowListLookupNode, RowPointLookupNode, RowRangeScanNode, ScalarizeNode, ScanNode, SortNode,
			TakeNode, VariableSourceNode, WindowNode,
		},
	},
};

impl PlanCompiler {
	pub(crate) fn compile_scan<'bump>(&mut self, node: &ScanNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		let source_name = match &node.primitive {
			Primitive::Table(t) => format!("{}.{}", t.namespace.name, t.name),
			Primitive::View(v) => format!("{}.{}", v.namespace.name, v.name),
			Primitive::RingBuffer(rb) => format!("{}.{}", rb.namespace.name, rb.name),
			Primitive::Dictionary(d) => format!("{}.{}", d.namespace.name, d.name),
		};

		let source_index = self.program.add_source(SourceDef {
			name: source_name,
			alias: node.alias.map(|s| s.to_string()),
		});

		self.writer.emit_opcode(Opcode::Source);
		self.writer.emit_u16(source_index);
		Ok(())
	}

	pub(crate) fn compile_index_scan<'bump>(&mut self, node: &IndexScanNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		// For now, treat index scan as regular scan
		// TODO: Add index hint handling
		let source_index = self.program.add_source(SourceDef {
			name: format!("{}.{}", node.primitive.namespace.name, node.primitive.name),
			alias: node.alias.map(|s| s.to_string()),
		});
		self.writer.emit_opcode(Opcode::Source);
		self.writer.emit_u16(source_index);
		Ok(())
	}

	pub(crate) fn compile_filter<'bump>(&mut self, node: &FilterNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile input pipeline
		self.compile_plan(node.input)?;

		// Compile predicate to CompiledFilter closure
		let compiled = compile_plan_filter(node.predicate);
		let filter_index = self.program.add_compiled_filter(compiled);

		// Push compiled filter reference
		self.writer.emit_opcode(Opcode::PushExpr);
		self.writer.emit_u16(filter_index);

		// Apply filter operator
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Filter as u8);
		Ok(())
	}

	pub(crate) fn compile_project<'bump>(&mut self, node: &ProjectNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile input if present
		if let Some(input) = node.input {
			self.compile_plan(input)?;
		}

		// Build column list from projections
		let columns: Vec<String> = node
			.projections
			.iter()
			.map(|p| {
				if let Some(alias) = p.alias {
					alias.to_string()
				} else {
					// Try to extract column name from expression
					self.expr_to_column_name(p.expr)
				}
			})
			.collect();

		let col_list_index = self.program.add_column_list(columns);
		self.writer.emit_opcode(Opcode::PushColList);
		self.writer.emit_u16(col_list_index);
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Select as u8);
		Ok(())
	}

	pub(crate) fn compile_extend<'bump>(&mut self, node: &ExtendNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile input if present
		if let Some(input) = node.input {
			self.compile_plan(input)?;
		}

		// Build extension spec with compiled expressions
		let mut spec = Vec::new();
		for ext in node.extensions.iter() {
			let name = ext.alias.unwrap_or("").to_string();
			// Compile expression to CompiledExpr closure
			let compiled = compile_plan_expr(ext.expr);
			let expr_index = self.program.add_compiled_expr(compiled);
			spec.push((name, expr_index));
		}

		let spec_index = self.program.add_extension_spec(spec);
		self.writer.emit_opcode(Opcode::PushExtSpec);
		self.writer.emit_u16(spec_index);
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Extend as u8);
		Ok(())
	}

	pub(crate) fn compile_aggregate<'bump>(&mut self, node: &AggregateNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.input)?;
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Aggregate as u8);
		Ok(())
	}

	pub(crate) fn compile_sort<'bump>(&mut self, node: &SortNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile input
		self.compile_plan(node.input)?;

		// Build sort spec
		let keys: Vec<SortKey> = node
			.keys
			.iter()
			.map(|k| SortKey {
				column: self.expr_to_column_name(k.expr),
				direction: match k.direction {
					crate::plan::node::query::SortDirection::Asc => SortDirection::Asc,
					crate::plan::node::query::SortDirection::Desc => SortDirection::Desc,
				},
				nulls: match k.nulls {
					crate::plan::node::query::NullsOrder::First => NullsOrder::First,
					crate::plan::node::query::NullsOrder::Last => NullsOrder::Last,
				},
			})
			.collect();

		let spec_index = self.program.add_sort_spec(SortSpec {
			keys,
		});
		self.writer.emit_opcode(Opcode::PushSortSpec);
		self.writer.emit_u16(spec_index);
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Sort as u8);
		Ok(())
	}

	pub(crate) fn compile_take<'bump>(&mut self, node: &TakeNode<'bump>) -> Result<()> {
		self.record_span(node.span);

		// Compile input
		self.compile_plan(node.input)?;

		// Push limit constant
		let const_index = self.program.add_constant(Constant::Int(node.count as i64));
		self.writer.emit_opcode(Opcode::PushConst);
		self.writer.emit_u16(const_index);

		// Apply take operator
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Take as u8);
		Ok(())
	}

	pub(crate) fn compile_distinct<'bump>(&mut self, node: &DistinctNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.input)?;
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::Distinct as u8);
		Ok(())
	}

	pub(crate) fn compile_join_inner<'bump>(&mut self, node: &JoinInnerNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.left)?;
		self.compile_plan(node.right)?;
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::JoinInner as u8);
		Ok(())
	}

	pub(crate) fn compile_join_left<'bump>(&mut self, node: &JoinLeftNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.left)?;
		self.compile_plan(node.right)?;
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::JoinLeft as u8);
		Ok(())
	}

	pub(crate) fn compile_join_natural<'bump>(&mut self, node: &JoinNaturalNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.left)?;
		self.compile_plan(node.right)?;
		self.writer.emit_opcode(Opcode::Apply);
		self.writer.emit_u8(OperatorKind::JoinNatural as u8);
		Ok(())
	}

	pub(crate) fn compile_merge<'bump>(&mut self, node: &MergeNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.left)?;
		self.compile_plan(node.right)?;
		self.writer.emit_opcode(Opcode::Merge);
		Ok(())
	}

	pub(crate) fn compile_window<'bump>(&mut self, node: &WindowNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		if let Some(input) = node.input {
			self.compile_plan(input)?;
		}
		// TODO: Implement window operation
		Err(CompileError::UnsupportedPlan {
			message: "window operations not yet supported".to_string(),
			span: node.span,
		})
	}

	pub(crate) fn compile_apply<'bump>(&mut self, node: &ApplyNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		if let Some(input) = node.input {
			self.compile_plan(input)?;
		}
		// TODO: Implement custom apply operator
		Err(CompileError::UnsupportedPlan {
			message: format!("apply operator '{}' not yet supported", node.operator),
			span: node.span,
		})
	}

	// ─────────────────────────────────────────────────────────────
	// Optimized Row Access
	// ─────────────────────────────────────────────────────────────

	pub(crate) fn compile_row_point_lookup<'bump>(&mut self, node: &RowPointLookupNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		// TODO: Implement point lookup optimization
		Err(CompileError::UnsupportedPlan {
			message: "row point lookup not yet supported".to_string(),
			span: node.span,
		})
	}

	pub(crate) fn compile_row_list_lookup<'bump>(&mut self, node: &RowListLookupNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		Err(CompileError::UnsupportedPlan {
			message: "row list lookup not yet supported".to_string(),
			span: node.span,
		})
	}

	pub(crate) fn compile_row_range_scan<'bump>(&mut self, node: &RowRangeScanNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		Err(CompileError::UnsupportedPlan {
			message: "row range scan not yet supported".to_string(),
			span: node.span,
		})
	}

	// ─────────────────────────────────────────────────────────────
	// Other Query
	// ─────────────────────────────────────────────────────────────

	pub(crate) fn compile_inline_data<'bump>(&mut self, node: &InlineDataNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.writer.emit_opcode(Opcode::Inline);
		// TODO: Push inline data rows
		Ok(())
	}

	pub(crate) fn compile_generator<'bump>(&mut self, node: &GeneratorNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		Err(CompileError::UnsupportedPlan {
			message: format!("generator '{}' not yet supported", node.name),
			span: node.span,
		})
	}

	pub(crate) fn compile_variable_source<'bump>(&mut self, node: &VariableSourceNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		// Load pipeline using variable ID
		self.writer.emit_opcode(Opcode::LoadPipeline);
		self.writer.emit_u32(node.variable.variable_id);
		Ok(())
	}

	pub(crate) fn compile_environment<'bump>(
		&mut self,
		node: &crate::plan::node::query::EnvironmentNode,
	) -> Result<()> {
		self.record_span(node.span);
		Err(CompileError::UnsupportedPlan {
			message: "environment source not yet supported".to_string(),
			span: node.span,
		})
	}

	pub(crate) fn compile_scalarize<'bump>(&mut self, node: &ScalarizeNode<'bump>) -> Result<()> {
		self.record_span(node.span);
		self.compile_plan(node.input)?;
		// Collect and extract single value
		self.writer.emit_opcode(Opcode::Collect);
		// TODO: Extract scalar from 1x1 result
		Ok(())
	}
}
