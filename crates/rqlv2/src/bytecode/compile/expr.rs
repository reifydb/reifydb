// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Expression compilation.

use crate::{
	bytecode::{
		compile::{CompileError, PlanCompiler, Result},
		opcode::Opcode,
		program::Constant,
	},
	plan::node::expr::{BinaryPlanOp, PlanExpr, UnaryPlanOp},
};

impl PlanCompiler {
	pub(crate) fn compile_expr<'bump>(&mut self, expr: &PlanExpr<'bump>) -> Result<()> {
		match expr {
			PlanExpr::LiteralNull(span) => {
				self.record_span(*span);
				let const_index = self.program.add_constant(Constant::Null);
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			PlanExpr::LiteralBool(value, span) => {
				self.record_span(*span);
				let const_index = self.program.add_constant(Constant::Bool(*value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			PlanExpr::LiteralInt(value, span) => {
				self.record_span(*span);
				let const_index = self.program.add_constant(Constant::Int(*value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			PlanExpr::LiteralFloat(value, span) => {
				self.record_span(*span);
				let const_index = self.program.add_constant(Constant::Float(*value));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			PlanExpr::LiteralString(value, span) => {
				self.record_span(*span);
				let const_index = self.program.add_constant(Constant::String(value.to_string()));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			PlanExpr::LiteralBytes(value, span) => {
				self.record_span(*span);
				let const_index = self.program.add_constant(Constant::Bytes(value.to_vec()));
				self.writer.emit_opcode(Opcode::PushConst);
				self.writer.emit_u16(const_index);
			}
			PlanExpr::Column(col) => {
				self.record_span(col.span());
				let name_index = self.program.add_constant(Constant::String(col.name().to_string()));
				self.writer.emit_opcode(Opcode::PushColRef);
				self.writer.emit_u16(name_index);
			}
			PlanExpr::Variable(var) => {
				self.record_span(var.span);
				self.writer.emit_opcode(Opcode::LoadVar);
				self.writer.emit_u32(var.variable_id);
			}
			PlanExpr::Rownum(span) => {
				self.record_span(*span);
				// TODO: Implement rownum pseudo-column
				return Err(CompileError::UnsupportedExpr {
					message: "rownum not yet supported".to_string(),
					span: *span,
				});
			}
			PlanExpr::Wildcard(span) => {
				self.record_span(*span);
				// Wildcard should be expanded during planning
				return Err(CompileError::UnsupportedExpr {
					message: "wildcard should be expanded during planning".to_string(),
					span: *span,
				});
			}
			PlanExpr::Binary {
				op,
				left,
				right,
				span,
			} => {
				self.record_span(*span);
				self.compile_expr(left)?;
				self.compile_expr(right)?;
				let opcode = match op {
					BinaryPlanOp::Add => Opcode::ColAdd,
					BinaryPlanOp::Sub => Opcode::ColSub,
					BinaryPlanOp::Mul => Opcode::ColMul,
					BinaryPlanOp::Div => Opcode::ColDiv,
					BinaryPlanOp::Eq => Opcode::ColEq,
					BinaryPlanOp::Ne => Opcode::ColNe,
					BinaryPlanOp::Lt => Opcode::ColLt,
					BinaryPlanOp::Le => Opcode::ColLe,
					BinaryPlanOp::Gt => Opcode::ColGt,
					BinaryPlanOp::Ge => Opcode::ColGe,
					BinaryPlanOp::And => Opcode::ColAnd,
					BinaryPlanOp::Or => Opcode::ColOr,
					BinaryPlanOp::Rem | BinaryPlanOp::Xor | BinaryPlanOp::Concat => {
						return Err(CompileError::UnsupportedExpr {
							message: format!("binary operator {:?} not yet supported", op),
							span: *span,
						});
					}
				};
				self.writer.emit_opcode(opcode);
			}
			PlanExpr::Unary {
				op,
				operand,
				span,
			} => {
				self.record_span(*span);
				self.compile_expr(operand)?;
				match op {
					UnaryPlanOp::Not => self.writer.emit_opcode(Opcode::ColNot),
					UnaryPlanOp::Neg | UnaryPlanOp::Plus => {
						return Err(CompileError::UnsupportedExpr {
							message: format!("unary operator {:?} not yet supported", op),
							span: *span,
						});
					}
				}
			}
			PlanExpr::Between {
				expr,
				low,
				high,
				negated,
				span,
			} => {
				self.record_span(*span);
				// Expand to: expr >= low AND expr <= high (or NOT of that)
				self.compile_expr(expr)?;
				self.compile_expr(low)?;
				self.writer.emit_opcode(Opcode::ColGe);
				self.compile_expr(expr)?;
				self.compile_expr(high)?;
				self.writer.emit_opcode(Opcode::ColLe);
				self.writer.emit_opcode(Opcode::ColAnd);
				if *negated {
					self.writer.emit_opcode(Opcode::ColNot);
				}
			}
			PlanExpr::In {
				expr,
				list,
				negated,
				span,
			} => {
				self.record_span(*span);
				// Expand to: expr = v1 OR expr = v2 OR ...
				if list.is_empty() {
					let const_index = self.program.add_constant(Constant::Bool(*negated));
					self.writer.emit_opcode(Opcode::PushConst);
					self.writer.emit_u16(const_index);
				} else {
					self.compile_expr(expr)?;
					self.compile_expr(list[0])?;
					self.writer.emit_opcode(Opcode::ColEq);
					for item in &list[1..] {
						self.compile_expr(expr)?;
						self.compile_expr(item)?;
						self.writer.emit_opcode(Opcode::ColEq);
						self.writer.emit_opcode(Opcode::ColOr);
					}
					if *negated {
						self.writer.emit_opcode(Opcode::ColNot);
					}
				}
			}
			PlanExpr::Cast {
				expr,
				target_type: _,
				span,
			} => {
				self.record_span(*span);
				// TODO: Implement type casting
				self.compile_expr(expr)?;
			}
			PlanExpr::Call {
				function,
				arguments,
				span,
			} => {
				self.record_span(*span);
				// Push arguments onto stack
				for arg in arguments.iter() {
					self.compile_expr(arg)?;
				}
				// Store function name in constant pool and emit call
				let name_index = self.program.add_constant(Constant::String(function.name.to_string()));
				self.writer.emit_opcode(Opcode::CallBuiltin);
				self.writer.emit_u16(name_index);
				self.writer.emit_u8(arguments.len() as u8);
			}
			PlanExpr::Aggregate {
				function,
				arguments,
				distinct: _,
				span,
			} => {
				self.record_span(*span);
				for arg in arguments.iter() {
					self.compile_expr(arg)?;
				}
				let _ = function;
				// Aggregate functions handled by Apply(Aggregate)
			}
			PlanExpr::Conditional {
				condition,
				then_expr,
				else_expr,
				span,
			} => {
				self.record_span(*span);
				self.compile_expr(condition)?;
				self.writer.emit_opcode(Opcode::JumpIfNot);
				let else_jump = self.writer.position();
				self.writer.emit_u16(0);
				self.compile_expr(then_expr)?;
				self.writer.emit_opcode(Opcode::Jump);
				let end_jump = self.writer.position();
				self.writer.emit_u16(0);
				self.writer.patch_jump(else_jump);
				self.compile_expr(else_expr)?;
				self.writer.patch_jump(end_jump);
			}
			PlanExpr::Subquery(plan) => {
				// Compile nested plan
				self.compile_plan(plan)?;
				self.writer.emit_opcode(Opcode::Collect);
			}
			PlanExpr::List(items, span) => {
				self.record_span(*span);
				for item in items.iter() {
					self.compile_expr(item)?;
				}
				// TODO: Build list value
			}
			PlanExpr::Tuple(items, span) => {
				self.record_span(*span);
				for item in items.iter() {
					self.compile_expr(item)?;
				}
				// TODO: Build tuple value
			}
			PlanExpr::Record(fields, span) => {
				self.record_span(*span);
				for (_, expr) in fields.iter() {
					self.compile_expr(expr)?;
				}
				// TODO: Build record value
			}
			PlanExpr::Alias {
				expr,
				alias: _,
				span,
			} => {
				self.record_span(*span);
				self.compile_expr(expr)?;
				// Alias is metadata, not runtime
			}
		}
		Ok(())
	}

	/// Extract column name from expression (for column lists).
	pub(crate) fn expr_to_column_name<'bump>(&self, expr: &PlanExpr<'bump>) -> String {
		match expr {
			PlanExpr::Column(col) => col.name().to_string(),
			PlanExpr::Variable(var) => var.name.to_string(),
			PlanExpr::Alias {
				alias,
				..
			} => alias.to_string(),
			_ => "<expr>".to_string(),
		}
	}
}
