// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This code includes parts derived from wain,
// available under the MIT License, with modifications.
//
// Copyright (c) 2020 rhysed
// Licensed under the MIT License (https://opensource.org/licenses/MIT)

use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Sub};

use crate::{
	execute::exec::Exec,
	module::{
		BranchingDepth, PAGE_SIZE, Trap, TrapDivisionByZero, TrapNotImplemented, TrapOutOfRange, TrapOverflow,
		types::{Instruction, ValueType},
		value::Value,
	},
};

pub enum ExecStatus {
	Break(BranchingDepth),
	Continue,
	Return,
}

pub type ExecResult = Result<ExecStatus, Trap>;

pub trait ExecInstruction {
	fn execute(&self, exec: &mut Exec) -> ExecResult;
}

impl ExecInstruction for [Instruction] {
	fn execute(&self, exec: &mut Exec) -> ExecResult {
		for instruction in self {
			match instruction.execute(exec)? {
				ExecStatus::Continue => {}
				state => return Ok(state),
			}
		}
		Ok(ExecStatus::Continue)
	}
}

impl ExecInstruction for Instruction {
	fn execute(&self, exec: &mut Exec) -> ExecResult {
		match self {
			Instruction::F32Abs => exec.unary::<f32, _>(FloatExtension::abs)?,
			Instruction::F64Abs => exec.unary::<f64, _>(FloatExtension::abs)?,
			Instruction::I32Add => exec.binary(i32::wrapping_add)?,
			Instruction::I64Add => exec.binary(i64::wrapping_add)?,
			Instruction::F32Add => exec.binary(f32::add)?,
			Instruction::F64Add => exec.binary(f64::add)?,

			Instruction::I32And => exec.binary(i32::bitand)?,
			Instruction::I64And => exec.binary(i64::bitand)?,

			Instruction::Block {
				result_types,
				body,
			} => {
				let stack_pointer = exec.stack.pointer();
				match body.execute(exec)? {
					ExecStatus::Continue => {}
					ExecStatus::Return => return Ok(ExecStatus::Return),
					ExecStatus::Break(0) => exec.stack.revert(stack_pointer, result_types)?,
					ExecStatus::Break(depth) => return Ok(ExecStatus::Break(depth - 1)),
				}
			}
			Instruction::Br(depth) => return Ok(ExecStatus::Break(*depth)),
			Instruction::BrIf(depth) => {
				let cond = exec.stack.pop::<i32>()?;
				if cond != 0 {
					return Ok(ExecStatus::Break(*depth));
				}
			}
			Instruction::BrTable {
				cases,
				default,
			} => {
				let idx = exec.stack.pop::<i32>()? as u32;
				let depth = idx as usize;
				let depth = if depth < cases.len() {
					cases[depth]
				} else {
					*default
				};
				return Ok(ExecStatus::Break(depth));
			}

			Instruction::Loop {
				param_types,
				body,
				..
			} => {
				let stack_pointer = exec.stack.pointer();
				loop {
					match body.execute(exec)? {
						ExecStatus::Continue => break,
						ExecStatus::Return => return Ok(ExecStatus::Return),
						ExecStatus::Break(0) => {
							// Branch back to loop — preserve param values for next
							// iteration
							exec.stack.revert(stack_pointer.clone(), param_types)?;
						}
						ExecStatus::Break(depth) => return Ok(ExecStatus::Break(depth - 1)),
					}
				}
			}

			Instruction::If {
				result_types,
				then,
				otherwise,
			} => {
				let cond: i32 = exec.stack.pop()?;
				let stack_pointer = exec.stack.pointer();
				let body = if cond != 0 {
					then
				} else {
					otherwise
				};

				let result = body.execute(exec)?;
				match result {
					ExecStatus::Continue => {}
					ExecStatus::Return => return Ok(ExecStatus::Return),
					ExecStatus::Break(0) => exec.stack.revert(stack_pointer, result_types)?,
					ExecStatus::Break(depth) => return Ok(ExecStatus::Break(depth - 1)),
				}
			}

			Instruction::Call(idx) => exec.call(idx)?,
			Instruction::CallIndirect(type_idx, table_idx) => exec.call_indirect(*type_idx, *table_idx)?,

			Instruction::F32Ceil => exec.unary::<f32, _>(FloatExtension::ceil)?,
			Instruction::F64Ceil => exec.unary::<f64, _>(FloatExtension::ceil)?,
			Instruction::I32Clz => exec.unary(|v: i32| v.leading_zeros() as i32)?,
			Instruction::I64Clz => exec.unary(|v: i64| v.leading_zeros() as i64)?,

			Instruction::I32Const(value) => exec.stack.push(Value::I32(value.clone()))?,
			Instruction::I64Const(value) => exec.stack.push(Value::I64(value.clone()))?,
			Instruction::F32Const(value) => exec.stack.push(Value::F32(value.clone()))?,
			Instruction::F64Const(value) => exec.stack.push(Value::F64(value.clone()))?,

			Instruction::F32ConvertI32S => exec.unary_map(|v: i32| v as f32)?,
			Instruction::F32ConvertI32U => exec.unary_map(|v: u32| v as f32)?,
			Instruction::F32ConvertI64S => exec.unary_map(|v: i64| v as f32)?,
			Instruction::F32ConvertI64U => exec.unary_map(|v: u64| v as f32)?,
			Instruction::F64ConvertI32S => exec.unary_map(|v: i32| v as f64)?,
			Instruction::F64ConvertI32U => exec.unary_map(|v: u32| v as f64)?,
			Instruction::F64ConvertI64S => exec.unary_map(|v: i64| v as f64)?,
			Instruction::F64ConvertI64U => exec.unary_map(|v: u64| v as f64)?,

			Instruction::F32Copysign => exec.binary::<f32, _>(FloatExtension::copysign)?,
			Instruction::F64Copysign => exec.binary::<f64, _>(FloatExtension::copysign)?,

			Instruction::I32Ctz => exec.unary(|v: i32| v.trailing_zeros() as i32)?,
			Instruction::I64Ctz => exec.unary(|v: i64| v.trailing_zeros() as i64)?,

			Instruction::F32DemoteF64 => exec.unary_map(|v: f64| v as f32)?,

			Instruction::I32DivS => exec.binary_trap(i32::div_checked)?,
			Instruction::I64DivS => exec.binary_trap(i64::div_checked)?,

			Instruction::I32DivU => exec.binary_trap(u32::div_checked)?,
			Instruction::I64DivU => exec.binary_trap(u64::div_checked)?,
			Instruction::F32Div => exec.binary(f32::div)?,
			Instruction::F64Div => exec.binary(f64::div)?,

			Instruction::Drop => {
				exec.stack.pop::<Value>()?;
			}

			Instruction::I32Eq => exec.binary_test(|l: i32, r| l == r)?,
			Instruction::I64Eq => exec.binary_test(|l: i64, r| l == r)?,
			Instruction::F32Eq => exec.binary_test(|l: f32, r| l == r)?,
			Instruction::F64Eq => exec.binary_test(|l: f64, r| l == r)?,
			Instruction::I32Eqz => exec.unary_test(|v: i32| v == 0)?,
			Instruction::I64Eqz => exec.unary_test(|v: i64| v == 0)?,

			Instruction::I64ExtendI32S => exec.unary_map(|v: i32| i64::from(v))?,
			Instruction::I64ExtendI32U => exec.unary_map(|v: u32| i64::from(v))?,
			Instruction::I32Extend8S => exec.unary_map(|v: i32| i32::from(v as i8))?,
			Instruction::I64Extend8S => exec.unary_map(|v: i64| i64::from(v as i8))?,
			Instruction::I32Extend16S => exec.unary_map(|v: i32| i32::from(v as i16))?,
			Instruction::I64Extend16S => exec.unary_map(|v: i64| i64::from(v as i16))?,
			Instruction::I64Extend32S => exec.unary_map(|v: i64| i64::from(v as i32))?,

			Instruction::F32Floor => exec.unary::<f32, _>(FloatExtension::floor)?,
			Instruction::F64Floor => exec.unary::<f64, _>(FloatExtension::floor)?,

			Instruction::I32GeS => exec.binary_test(|l: i32, r| l >= r)?,
			Instruction::I64GeS => exec.binary_test(|l: i64, r| l >= r)?,
			Instruction::I32GeU => exec.binary_test(|l: i32, r| (l as u32) >= r as u32)?,
			Instruction::I64GeU => exec.binary_test(|l: i64, r| (l as u64) >= r as u64)?,
			Instruction::F32Ge => exec.binary_test(|l: f32, r| l >= r)?,
			Instruction::F64Ge => exec.binary_test(|l: f64, r| l >= r)?,

			Instruction::GlobalGet(idx) => exec.global_get(*idx)?,
			Instruction::GlobalSet(idx) => exec.global_set(*idx)?,

			Instruction::I32GtS => exec.binary_test(|l: i32, r| l > r)?,
			Instruction::I64GtS => exec.binary_test(|l: i64, r| l > r)?,
			Instruction::I32GtU => exec.binary_test(|l: i32, r| (l as u32) > r as u32)?,
			Instruction::I64GtU => exec.binary_test(|l: i64, r| (l as u64) > r as u64)?,
			Instruction::F32Gt => exec.binary_test(|l: f32, r| l > r)?,
			Instruction::F64Gt => exec.binary_test(|l: f64, r| l > r)?,

			Instruction::I32LeS => exec.binary_test(|l: i32, r| l <= r)?,
			Instruction::I64LeS => exec.binary_test(|l: i64, r| l <= r)?,
			Instruction::I32LeU => exec.binary_test(|l: i32, r| (l as u32) <= r as u32)?,
			Instruction::I64LeU => exec.binary_test(|l: i64, r| (l as u64) <= r as u64)?,
			Instruction::F32Le => exec.binary_test(|l: f32, r| l <= r)?,
			Instruction::F64Le => exec.binary_test(|l: f64, r| l <= r)?,

			Instruction::I32Load8S(mem) => exec.load::<i32, i8>(mem)?,
			Instruction::I32Load8U(mem) => exec.load::<i32, u8>(mem)?,
			Instruction::I32Load16S(mem) => exec.load::<i32, i16>(mem)?,
			Instruction::I32Load16U(mem) => exec.load::<i32, u16>(mem)?,
			Instruction::I32Load(mem) => exec.load::<i32, i32>(mem)?,

			Instruction::I64Load8S(mem) => exec.load::<i64, i8>(mem)?,
			Instruction::I64Load8U(mem) => exec.load::<i64, u8>(mem)?,
			Instruction::I64Load16S(mem) => exec.load::<i64, i16>(mem)?,
			Instruction::I64Load16U(mem) => exec.load::<i64, u16>(mem)?,
			Instruction::I64Load32S(mem) => exec.load::<i64, i32>(mem)?,
			Instruction::I64Load32U(mem) => exec.load::<i64, u32>(mem)?,
			Instruction::I64Load(mem) => exec.load::<i64, i64>(mem)?,

			Instruction::F32Load(mem) => exec.load::<f32, f32>(mem)?,
			Instruction::F64Load(mem) => exec.load::<f64, f64>(mem)?,

			Instruction::LocalGet(idx) => exec.local_get(*idx)?,
			Instruction::LocalSet(idx) => exec.local_set(*idx)?,
			Instruction::LocalTee(idx) => exec.local_tee(*idx)?,

			Instruction::I32LtS => exec.binary_test(|l: i32, r| l < r)?,
			Instruction::I64LtS => exec.binary_test(|l: i64, r| l < r)?,
			Instruction::I32LtU => exec.binary_test(|l: i32, r| (l as u32) < r as u32)?,
			Instruction::I64LtU => exec.binary_test(|l: i64, r| (l as u64) < r as u64)?,
			Instruction::F32Lt => exec.binary_test(|l: f32, r| l < r)?,
			Instruction::F64Lt => exec.binary_test(|l: f64, r| l < r)?,

			Instruction::F32Max => exec.binary::<f32, _>(FloatExtension::max)?,
			Instruction::F64Max => exec.binary::<f64, _>(FloatExtension::max)?,

			Instruction::MemoryGrow(_) => {
				let pages: i32 = exec.stack.pop()?;
				match exec.memory_grow(pages as u32) {
					Ok(prev_pages) => exec.stack.push(prev_pages as i32)?,
					Err(_) => exec.stack.push(-1i32)?,
				}
			}

			Instruction::MemorySize(_) => {
				let mem_rc = exec.state.memory_rc(0)?;
				let memory = mem_rc.borrow();
				let pages = (memory.len() / PAGE_SIZE as usize) as i32;
				exec.stack.push(pages)?;
			}

			Instruction::MemoryCopy => {
				let len: i32 = exec.stack.pop()?;
				let src: i32 = exec.stack.pop()?;
				let dst: i32 = exec.stack.pop()?;
				exec.state.memory_copy(dst as usize, src as usize, len as usize)?;
			}

			Instruction::MemoryFill => {
				let len: i32 = exec.stack.pop()?;
				let val: i32 = exec.stack.pop()?;
				let dst: i32 = exec.stack.pop()?;
				exec.state.memory_fill(dst as usize, val as u8, len as usize)?;
			}

			Instruction::MemoryInit(data_idx) => {
				let len: i32 = exec.stack.pop()?;
				let src: i32 = exec.stack.pop()?;
				let dst: i32 = exec.stack.pop()?;
				exec.state.memory_init(*data_idx, dst as usize, src as usize, len as usize)?;
			}

			Instruction::DataDrop(data_idx) => {
				exec.state.data_drop(*data_idx)?;
			}

			Instruction::TableGet(table_idx) => {
				let idx: i32 = exec.stack.pop()?;
				let value = exec.state.table_at(*table_idx, idx as usize)?;
				exec.stack.push(value)?;
			}

			Instruction::TableSet(table_idx) => {
				let value: Value = exec.stack.pop()?;
				let idx: i32 = exec.stack.pop()?;
				let table_rc = exec.state.table_rc(*table_idx)?;
				let mut table = table_rc.borrow_mut();
				if idx as usize >= table.elements.len() {
					return Err(Trap::OutOfRange(TrapOutOfRange::Table(*table_idx)));
				}
				// Resolve func_ref if setting a RefFunc value
				let func_ref = if let Value::RefFunc(func_idx) = &value {
					exec.state.functions.get(*func_idx).cloned()
				} else {
					None
				};
				table.elements[idx as usize] = Some(value);
				table.func_refs[idx as usize] = func_ref;
			}

			Instruction::TableGrow(table_idx) => {
				let n: i32 = exec.stack.pop()?;
				let init: Value = exec.stack.pop()?;
				let result = exec.state.table_grow(*table_idx, n as u32, init)?;
				exec.stack.push(result)?;
			}

			Instruction::TableSize(table_idx) => {
				let size = exec.state.table_size(*table_idx)?;
				exec.stack.push(size as i32)?;
			}

			Instruction::TableFill(table_idx) => {
				let len: i32 = exec.stack.pop()?;
				let val: Value = exec.stack.pop()?;
				let dst: i32 = exec.stack.pop()?;
				exec.state.table_fill(*table_idx, dst as u32, val, len as u32)?;
			}

			Instruction::TableCopy(dst_idx, src_idx) => {
				let len: i32 = exec.stack.pop()?;
				let src: i32 = exec.stack.pop()?;
				let dst: i32 = exec.stack.pop()?;
				exec.state.table_copy(*dst_idx, *src_idx, dst as u32, src as u32, len as u32)?;
			}

			Instruction::TableInit(table_idx, elem_idx) => {
				let len: i32 = exec.stack.pop()?;
				let src: i32 = exec.stack.pop()?;
				let dst: i32 = exec.stack.pop()?;
				exec.state.table_init(*table_idx, *elem_idx, dst as u32, src as u32, len as u32)?;
			}

			Instruction::ElemDrop(elem_idx) => {
				exec.state.elem_drop(*elem_idx)?;
			}

			Instruction::RefNull(vt) => {
				exec.stack.push(Value::RefNull(vt.clone()))?;
			}

			Instruction::RefIsNull => {
				let val: Value = exec.stack.pop()?;
				let is_null = matches!(val, Value::RefNull(_));
				exec.stack.push(if is_null {
					Value::I32(1)
				} else {
					Value::I32(0)
				})?;
			}

			Instruction::RefFunc(func_idx) => {
				exec.stack.push(Value::RefFunc(*func_idx))?;
			}

			Instruction::F32Min => exec.binary::<f32, _>(FloatExtension::min)?,
			Instruction::F64Min => exec.binary::<f64, _>(FloatExtension::min)?,

			Instruction::I32Mul => exec.binary(i32::wrapping_mul)?,
			Instruction::I64Mul => exec.binary(i64::wrapping_mul)?,
			Instruction::F32Mul => exec.binary(f32::mul)?,
			Instruction::F64Mul => exec.binary(f64::mul)?,

			Instruction::I32Ne => exec.binary_test(|l: i32, r| l != r)?,
			Instruction::I64Ne => exec.binary_test(|l: i64, r| l != r)?,
			Instruction::F32Ne => exec.binary_test(|l: f32, r| l != r)?,
			Instruction::F64Ne => exec.binary_test(|l: f64, r| l != r)?,

			Instruction::F32Nearest => exec.unary::<f32, _>(FloatExtension::nearest)?,
			Instruction::F64Nearest => exec.unary::<f64, _>(FloatExtension::nearest)?,
			Instruction::F32Neg => exec.unary(f32::neg)?,
			Instruction::F64Neg => exec.unary(f64::neg)?,

			Instruction::Nop => {}

			Instruction::I32Or => exec.binary(i32::bitor)?,
			Instruction::I64Or => exec.binary(i64::bitor)?,

			Instruction::F64PromoteF32 => exec.unary_map(|v: f32| v as f64)?,

			Instruction::I32Popcnt => exec.unary(|v: i32| v.count_ones() as i32)?,
			Instruction::I64Popcnt => exec.unary(|v: i64| v.count_ones() as i64)?,

			Instruction::I32ReinterpretF32 => exec.stack.replace_type(ValueType::I32)?,
			Instruction::F32ReinterpretI32 => exec.stack.replace_type(ValueType::F32)?,
			Instruction::I64ReinterpretF64 => exec.stack.replace_type(ValueType::I64)?,
			Instruction::F64ReinterpretI64 => exec.stack.replace_type(ValueType::F64)?,

			Instruction::I32RemS => exec.binary_trap(i32::rem_wrapping)?,
			Instruction::I64RemS => exec.binary_trap(i64::rem_wrapping)?,

			Instruction::I32RemU => exec.binary_trap(u32::rem_wrapping)?,
			Instruction::I64RemU => exec.binary_trap(u64::rem_wrapping)?,

			Instruction::Return => return Ok(ExecStatus::Return),

			Instruction::I32Rotl => exec.binary(|l: i32, r| l.rotate_left(r as u32))?,
			Instruction::I64Rotl => exec.binary(|l: i64, r| l.rotate_left(r as u32))?,

			Instruction::I32Rotr => exec.binary(|l: i32, r| l.rotate_right(r as u32))?,
			Instruction::I64Rotr => exec.binary(|l: i64, r| l.rotate_right(r as u32))?,

			Instruction::Select => {
				let cond: i32 = exec.stack.pop()?;
				let value: Value = exec.stack.pop()?;
				if cond == 0 {
					exec.stack.pop::<Value>()?;
					exec.stack.push(value)?;
				}
			}

			Instruction::I32Store8(mem) => {
				let value = exec.stack.pop::<i32>()?;
				exec.store(mem, value as i8)?
			}
			Instruction::I32Store16(mem) => {
				let value = exec.stack.pop::<i32>()?;
				exec.store(mem, value as i16)?
			}
			Instruction::I32Store(mem) => {
				let value = exec.stack.pop::<i32>()?;
				exec.store(mem, value)?
			}

			Instruction::I64Store8(mem) => {
				let value = exec.stack.pop::<i64>()?;
				exec.store(mem, value as i8)?
			}
			Instruction::I64Store16(mem) => {
				let value = exec.stack.pop::<i64>()?;
				exec.store(mem, value as i16)?
			}
			Instruction::I64Store32(mem) => {
				let value = exec.stack.pop::<i64>()?;
				exec.store(mem, value as i32)?
			}
			Instruction::I64Store(mem) => {
				let value = exec.stack.pop::<i64>()?;
				exec.store(mem, value)?
			}

			Instruction::F32Store(mem) => {
				let value = exec.stack.pop::<f32>()?;
				exec.store(mem, value)?
			}

			Instruction::F64Store(mem) => {
				let value = exec.stack.pop::<f64>()?;
				exec.store(mem, value)?
			}

			Instruction::I32Shl => exec.binary(|l: i32, r| l.wrapping_shl(r as u32))?,
			Instruction::I64Shl => exec.binary(|l: i64, r| l.wrapping_shl(r as u32))?,

			Instruction::I32ShrS => exec.binary(|l: i32, r| l.wrapping_shr(r as u32))?,
			Instruction::I64ShrS => exec.binary(|l: i64, r| l.wrapping_shr(r as u32))?,

			Instruction::I32ShrU => exec.binary(|l: u32, r| l.wrapping_shr(r))?,
			Instruction::I64ShrU => exec.binary(|l: u64, r| l.wrapping_shr(r as u32))?,

			Instruction::F32Sqrt => exec.unary(f32::sqrt)?,
			Instruction::F64Sqrt => exec.unary(f64::sqrt)?,

			Instruction::I32Sub => exec.binary(i32::wrapping_sub)?,
			Instruction::I64Sub => exec.binary(i64::wrapping_sub)?,
			Instruction::F32Sub => exec.binary(f32::sub)?,
			Instruction::F64Sub => exec.binary(f64::sub)?,

			Instruction::F32Trunc => exec.unary::<f32, _>(FloatExtension::trunc)?,
			Instruction::F64Trunc => exec.unary::<f64, _>(FloatExtension::trunc)?,
			Instruction::I32TruncF32S => exec.unary_trap(f32_to_i32)?,
			Instruction::I32TruncF32U => exec.unary_trap(f32_to_u32)?,
			Instruction::I32TruncF64S => exec.unary_trap(f64_to_i32)?,
			Instruction::I32TruncF64U => exec.unary_trap(f64_to_u32)?,
			Instruction::I64TruncF32S => exec.unary_trap(f32_to_i64)?,
			Instruction::I64TruncF32U => exec.unary_trap(f32_to_u64)?,
			Instruction::I64TruncF64S => exec.unary_trap(f64_to_i64)?,
			Instruction::I64TruncF64U => exec.unary_trap(f64_to_u64)?,

			Instruction::I32TruncSatF32S => exec.unary_map(f32_to_i32_sat)?,
			Instruction::I32TruncSatF32U => exec.unary_map(f32_to_u32_sat)?,
			Instruction::I32TruncSatF64S => exec.unary_map(f64_to_i32_sat)?,
			Instruction::I32TruncSatF64U => exec.unary_map(f64_to_u32_sat)?,
			Instruction::I64TruncSatF32S => exec.unary_map(f32_to_i64_sat)?,
			Instruction::I64TruncSatF32U => exec.unary_map(f32_to_u64_sat)?,
			Instruction::I64TruncSatF64S => exec.unary_map(f64_to_i64_sat)?,
			Instruction::I64TruncSatF64U => exec.unary_map(f64_to_u64_sat)?,

			Instruction::I32WrapI64 => exec.unary_map(|v: i64| v as i32)?,

			Instruction::Unreachable => return Err(Trap::Unreachable),

			Instruction::I32Xor => exec.binary(i32::bitxor)?,
			Instruction::I64Xor => exec.binary(i64::bitxor)?,

			_ => return Err(Trap::NotImplemented(TrapNotImplemented::Instruction(self.clone()))),
		}

		Ok(ExecStatus::Continue)
	}
}

pub(crate) trait IntegerExtension
where
	Self: Sized,
{
	fn div_checked(self, other: Self) -> Result<Self, Trap>;
	fn rem_wrapping(self, other: Self) -> Result<Self, Trap>;
}

impl IntegerExtension for i32 {
	fn div_checked(self, other: Self) -> Result<Self, Trap> {
		self.checked_div(other).ok_or_else(|| {
			if self == Self::MIN && other == -1 {
				Trap::Overflow(TrapOverflow::Integer)
			} else {
				Trap::DivisionByZero(TrapDivisionByZero::Integer)
			}
		})
	}

	fn rem_wrapping(self, other: Self) -> Result<Self, Trap> {
		if other == 0 {
			Err(Trap::DivisionByZero(TrapDivisionByZero::Integer))
		} else {
			Ok(self.wrapping_rem(other))
		}
	}
}

impl IntegerExtension for u32 {
	fn div_checked(self, other: Self) -> Result<Self, Trap> {
		self.checked_div(other).ok_or_else(|| Trap::DivisionByZero(TrapDivisionByZero::Integer))
	}

	fn rem_wrapping(self, other: Self) -> Result<Self, Trap> {
		if other == 0 {
			Err(Trap::DivisionByZero(TrapDivisionByZero::Integer))
		} else {
			Ok(self % other)
		}
	}
}

impl IntegerExtension for i64 {
	fn div_checked(self, other: Self) -> Result<Self, Trap> {
		self.checked_div(other).ok_or_else(|| {
			if self == Self::MIN && other == -1 {
				Trap::Overflow(TrapOverflow::Integer)
			} else {
				Trap::DivisionByZero(TrapDivisionByZero::Integer)
			}
		})
	}

	fn rem_wrapping(self, other: Self) -> Result<Self, Trap> {
		if other == 0 {
			Err(Trap::DivisionByZero(TrapDivisionByZero::Integer))
		} else {
			Ok(self.wrapping_rem(other))
		}
	}
}

impl IntegerExtension for u64 {
	fn div_checked(self, other: Self) -> Result<Self, Trap> {
		self.checked_div(other).ok_or_else(|| Trap::DivisionByZero(TrapDivisionByZero::Integer))
	}

	fn rem_wrapping(self, other: Self) -> Result<Self, Trap> {
		if other == 0 {
			Err(Trap::DivisionByZero(TrapDivisionByZero::Integer))
		} else {
			Ok(self % other)
		}
	}
}

pub(crate) trait FloatExtension
where
	Self: Sized,
{
	fn abs(self) -> Self;
	fn ceil(self) -> Self;
	fn copysign(self, other: Self) -> Self;
	fn floor(self) -> Self;
	fn max(self, other: Self) -> Self;
	fn min(self, other: Self) -> Self;
	fn nearest(self) -> Self;
	fn nan(self) -> Self;
	fn trunc(self) -> Self;
}

impl FloatExtension for f32 {
	fn abs(self) -> Self {
		libm::fabsf(self)
	}

	fn ceil(self) -> Self {
		libm::ceilf(self)
	}

	fn copysign(self, other: Self) -> Self {
		libm::copysignf(self, other)
	}

	fn floor(self) -> Self {
		libm::floorf(self)
	}

	fn min(self, other: Self) -> Self {
		if self.is_nan() {
			self.nan()
		} else if other.is_nan() {
			other.nan()
		} else if self == other {
			f32::from_bits(self.to_bits() | other.to_bits())
		} else {
			f32::min(self, other)
		}
	}

	fn max(self, other: Self) -> Self {
		if self.is_nan() {
			self.nan()
		} else if other.is_nan() {
			other.nan()
		} else if self == other {
			f32::from_bits(self.to_bits() & other.to_bits())
		} else {
			f32::max(self, other)
		}
	}

	fn nearest(self) -> Self {
		let fround = self.round();
		if (self - fround).abs() == 0.5 && fround % 2.0 != 0.0 {
			self.trunc()
		} else {
			fround
		}
	}

	fn trunc(self) -> Self {
		libm::truncf(self)
	}

	fn nan(self) -> Self {
		Self::from_bits(self.to_bits() | 1 << <f32>::MANTISSA_DIGITS - 2)
	}
}

impl FloatExtension for f64 {
	fn abs(self) -> Self {
		libm::fabs(self)
	}

	fn ceil(self) -> Self {
		libm::ceil(self)
	}

	fn copysign(self, other: Self) -> Self {
		libm::copysign(self, other)
	}

	fn floor(self) -> Self {
		libm::floor(self)
	}

	fn min(self, other: Self) -> Self {
		if self.is_nan() {
			self.nan()
		} else if other.is_nan() {
			other.nan()
		} else if self == other {
			f64::from_bits(self.to_bits() | other.to_bits())
		} else {
			f64::min(self, other)
		}
	}

	fn max(self, other: Self) -> Self {
		if self.is_nan() {
			self.nan()
		} else if other.is_nan() {
			other.nan()
		} else if self == other {
			f64::from_bits(self.to_bits() & other.to_bits())
		} else {
			f64::max(self, other)
		}
	}

	fn nearest(self) -> Self {
		let fround = self.round();
		if (self - fround).abs() == 0.5 && fround % 2.0 != 0.0 {
			self.trunc()
		} else {
			fround
		}
	}

	fn trunc(self) -> Self {
		libm::trunc(self)
	}

	fn nan(self) -> Self {
		Self::from_bits(self.to_bits() | 1 << <f64>::MANTISSA_DIGITS - 2)
	}
}

macro_rules! float_to_int {
	($name:ident, $float:ty, $int:ty) => {
		pub fn $name(f: $float) -> Result<$int, Trap> {
			const MAX: $float = <$int>::MAX as $float;
			const MIN: $float = <$int>::MIN as $float;

			if f.is_nan() {
				return Err(Trap::Conversion);
			}

			// Conceptually, f can be represented by the target type
			// if MIN - 1.0 < f < MAX + 1.0 is satisfied.
			// 1. f < MAX + 1.0 check. MAX + 1.0 can be represented exactly by the source type whether MAX
			//    can be represented exactly or not.  (Because MAX + 1.0 is always a power of 2.) So, we can
			//    simply check it with f < MAX + 1.0.
			// 2. MIN - 1.0 < f check. MIN can be represented exactly by any source types (because MIN is
			//    always a power of two.), but MIN - 1.0 is not. a) If MIN - 1.0 can be represented exactly
			//    too, then we can simply check it with MIN - 1.0 < f. (In this case, of course, MIN - 1.0
			//    != MIN.) b) If MIN - 1.0 can not be represented exactly, then MIN - 1.0 = MIN.  (Because
			//    of "round to nearest even" behavior.) So, we can check it with MIN <= f.
			let gt_min = if MIN != MIN - 1.0 {
				MIN - 1.0 < f
			} else {
				MIN <= f
			};

			if gt_min && f < MAX + 1.0 {
				return Ok(f as $int);
			}
			return Err(Trap::Overflow(TrapOverflow::Integer));
		}
	};
}

float_to_int!(f32_to_u32, f32, u32);
float_to_int!(f32_to_u64, f32, u64);
float_to_int!(f32_to_i64, f32, i64);
float_to_int!(f32_to_i32, f32, i32);
float_to_int!(f64_to_u32, f64, u32);
float_to_int!(f64_to_u64, f64, u64);
float_to_int!(f64_to_i32, f64, i32);
float_to_int!(f64_to_i64, f64, i64);

macro_rules! float_to_int_sat {
	($name:ident, $float:ty, $int:ty) => {
		pub fn $name(f: $float) -> $int {
			const MAX: $float = <$int>::MAX as $float;
			const MIN: $float = <$int>::MIN as $float;
			if f.is_nan() {
				0
			} else if f >= MAX {
				<$int>::MAX
			} else if f <= MIN {
				<$int>::MIN
			} else {
				f as $int
			}
		}
	};
}

float_to_int_sat!(f32_to_u32_sat, f32, u32);
float_to_int_sat!(f32_to_u64_sat, f32, u64);
float_to_int_sat!(f64_to_u32_sat, f64, u32);
float_to_int_sat!(f64_to_u64_sat, f64, u64);
float_to_int_sat!(f32_to_i32_sat, f32, i32);
float_to_int_sat!(f32_to_i64_sat, f32, i64);
float_to_int_sat!(f64_to_i32_sat, f64, i32);
float_to_int_sat!(f64_to_i64_sat, f64, i64);

#[cfg(test)]
mod tests {
	use super::*;

	trait NextAfter {
		fn next_after(self, _: Self) -> Self;
	}

	impl NextAfter for f32 {
		fn next_after(self, o: f32) -> f32 {
			let b = self.to_bits();
			f32::from_bits(if self.abs() < o.abs() {
				b + 1
			} else {
				b - 1
			})
		}
	}

	impl NextAfter for f64 {
		fn next_after(self, o: f64) -> f64 {
			let b = self.to_bits();
			f64::from_bits(if self.abs() < o.abs() {
				b + 1
			} else {
				b - 1
			})
		}
	}

	#[test]
	fn convert_f32_to_i32() {
		let conversion_error: Result<i32, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<i32, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f32_to_i32((i32::MIN as f32).next_after(f32::NEG_INFINITY)), overflow_error);
		assert_eq!(f32_to_i32(i32::MIN as f32), Ok(i32::MIN));
		assert_eq!(f32_to_i32(-1.0), Ok(-1));
		assert_eq!(f32_to_i32(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f32_to_i32(-0.0), Ok(0));
		assert_eq!(f32_to_i32(0.0), Ok(0));
		assert_eq!(f32_to_i32(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f32_to_i32(1.0), Ok(1));
		assert_eq!(f32_to_i32((i32::MAX as f32).next_after(0.0)), Ok(0x7fff_ff80));
		assert_eq!(f32_to_i32(i32::MAX as f32), overflow_error);
		assert_eq!(f32_to_i32(f32::INFINITY), overflow_error);
		assert_eq!(f32_to_i32(f32::NEG_INFINITY), overflow_error);
		assert_eq!(f32_to_i32(f32::NAN), conversion_error);
		assert_eq!(f32_to_i32(-f32::NAN), conversion_error);
	}

	#[test]
	fn convert_f32_to_i64() {
		let conversion_error: Result<i64, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<i64, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f32_to_i64((i64::MIN as f32).next_after(f32::NEG_INFINITY)), overflow_error);
		assert_eq!(f32_to_i64(i64::MIN as f32), Ok(i64::MIN));
		assert_eq!(f32_to_i64(-1.0), Ok(-1));
		assert_eq!(f32_to_i64(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f32_to_i64(-0.0), Ok(0));
		assert_eq!(f32_to_i64(0.0), Ok(0));
		assert_eq!(f32_to_i64(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f32_to_i64(1.0), Ok(1));
		assert_eq!(f32_to_i64((i64::MAX as f32).next_after(0.0)), Ok(0x7fff_ff80_0000_0000));
		assert_eq!(f32_to_i64(i64::MAX as f32), overflow_error);
		assert_eq!(f32_to_i64(f32::INFINITY), overflow_error);
		assert_eq!(f32_to_i64(f32::NEG_INFINITY), overflow_error);
		assert_eq!(f32_to_i64(f32::NAN), conversion_error);
		assert_eq!(f32_to_i64(-f32::NAN), conversion_error);
	}

	#[test]
	fn convert_f64_to_i32() {
		let conversion_error: Result<i32, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<i32, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f64_to_i32(i32::MIN as f64 - 1.0), overflow_error);
		assert_eq!(f64_to_i32((i32::MIN as f64 - 1.0).next_after(-0.0)), Ok(i32::MIN));
		assert_eq!(f64_to_i32(-1.0), Ok(-1));
		assert_eq!(f64_to_i32(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f64_to_i32(-0.0), Ok(0));
		assert_eq!(f64_to_i32(0.0), Ok(0));
		assert_eq!(f64_to_i32(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f64_to_i32(1.0), Ok(1));
		assert_eq!(f64_to_i32((i32::MAX as f64 + 1.0).next_after(0.0)), Ok(i32::MAX));
		assert_eq!(f64_to_i32(i32::MAX as f64 + 1.0), overflow_error);
		assert_eq!(f64_to_i32(f64::INFINITY), overflow_error);
		assert_eq!(f64_to_i32(f64::NEG_INFINITY), overflow_error);
		assert_eq!(f64_to_i32(f64::NAN), conversion_error);
		assert_eq!(f64_to_i32(-f64::NAN), conversion_error);
	}

	#[test]
	fn convert_f64_to_i64() {
		let conversion_error: Result<i64, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<i64, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f64_to_i64((i64::MIN as f64).next_after(f64::NEG_INFINITY)), overflow_error);
		assert_eq!(f64_to_i64(i64::MIN as f64), Ok(i64::MIN));
		assert_eq!(f64_to_i64(-1.0), Ok(-1));
		assert_eq!(f64_to_i64(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f64_to_i64(-0.0), Ok(0));
		assert_eq!(f64_to_i64(0.0), Ok(0));
		assert_eq!(f64_to_i64(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f64_to_i64(1.0), Ok(1));
		assert_eq!(f64_to_i64((i64::MAX as f64).next_after(0.0)), Ok(0x7fff_ffff_ffff_fc00));
		assert_eq!(f64_to_i64(i64::MAX as f64), overflow_error);
		assert_eq!(f64_to_i64(f64::INFINITY), overflow_error);
		assert_eq!(f64_to_i64(f64::NEG_INFINITY), overflow_error);
		assert_eq!(f64_to_i64(f64::NAN), conversion_error);
		assert_eq!(f64_to_i64(-f64::NAN), conversion_error);
	}

	#[test]
	fn convert_f32_to_u32() {
		let conversion_error: Result<u32, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<u32, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f32_to_u32(-1.0), overflow_error);
		assert_eq!(f32_to_u32(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f32_to_u32(-0.0), Ok(0));
		assert_eq!(f32_to_u32(0.0), Ok(0));
		assert_eq!(f32_to_u32(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f32_to_u32(1.0), Ok(1));
		assert_eq!(f32_to_u32((u32::MAX as f32).next_after(0.0)), Ok(0xffff_ff00));
		assert_eq!(f32_to_u32(u32::MAX as f32), overflow_error);
		assert_eq!(f32_to_u32(f32::INFINITY), overflow_error);
		assert_eq!(f32_to_u32(f32::NEG_INFINITY), overflow_error);
		assert_eq!(f32_to_u32(f32::NAN), conversion_error);
		assert_eq!(f32_to_u32(-f32::NAN), conversion_error);
	}

	#[test]
	fn convert_f32_to_u64() {
		let conversion_error: Result<u64, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<u64, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f32_to_u64(-1.0), overflow_error);
		assert_eq!(f32_to_u64(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f32_to_u64(-0.0), Ok(0));
		assert_eq!(f32_to_u64(0.0), Ok(0));
		assert_eq!(f32_to_u64(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f32_to_u64(1.0), Ok(1));
		assert_eq!(f32_to_u64((u64::MAX as f32).next_after(0.0)), Ok(0xffff_ff00_0000_0000));
		assert_eq!(f32_to_u64(u64::MAX as f32), overflow_error);
		assert_eq!(f32_to_u64(f32::INFINITY), overflow_error);
		assert_eq!(f32_to_u64(f32::NEG_INFINITY), overflow_error);
		assert_eq!(f32_to_u64(f32::NAN), conversion_error);
		assert_eq!(f32_to_u64(-f32::NAN), conversion_error);
	}

	#[test]
	fn convert_f64_to_u32() {
		let conversion_error: Result<u32, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<u32, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f64_to_u32(-1.0), overflow_error);
		assert_eq!(f64_to_u32(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f64_to_u32(-0.0), Ok(0));
		assert_eq!(f64_to_u32(0.0), Ok(0));
		assert_eq!(f64_to_u32(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f64_to_u32(1.0), Ok(1));
		assert_eq!(f64_to_u32((u32::MAX as f64 + 1.0).next_after(0.0)), Ok(u32::MAX));
		assert_eq!(f64_to_u32(u32::MAX as f64 + 1.0), overflow_error);
		assert_eq!(f64_to_u32(f64::INFINITY), overflow_error);
		assert_eq!(f64_to_u32(f64::NEG_INFINITY), overflow_error);
		assert_eq!(f64_to_u32(f64::NAN), conversion_error);
		assert_eq!(f64_to_u32(-f64::NAN), conversion_error);
	}

	#[test]
	fn convert_f64_to_u64() {
		let conversion_error: Result<u64, Trap> = Err(Trap::Conversion);
		let overflow_error: Result<u64, Trap> = Err(Trap::Overflow(TrapOverflow::Integer));

		assert_eq!(f64_to_u64(-1.0), overflow_error);
		assert_eq!(f64_to_u64(-(1.0.next_after(-0.0))), Ok(0));
		assert_eq!(f64_to_u64(-0.0), Ok(0));
		assert_eq!(f64_to_u64(0.0), Ok(0));
		assert_eq!(f64_to_u64(1.0.next_after(0.0)), Ok(0));
		assert_eq!(f64_to_u64(1.0), Ok(1));
		assert_eq!(f64_to_u64((u64::MAX as f64).next_after(0.0)), Ok(0xffff_ffff_ffff_f800));
		assert_eq!(f64_to_u64(u64::MAX as f64), overflow_error);
		assert_eq!(f64_to_u64(f64::INFINITY), overflow_error);
		assert_eq!(f64_to_u64(f64::NEG_INFINITY), overflow_error);
		assert_eq!(f64_to_u64(f64::NAN), conversion_error);
		assert_eq!(f64_to_u64(-f64::NAN), conversion_error);
	}

	#[test]
	fn convert_f32_to_i32_sat() {
		assert_eq!(f32_to_i32_sat(0.0), 0);
		assert_eq!(f32_to_i32_sat(i32::MIN as f32), i32::MIN);
		assert_eq!(f32_to_i32_sat(i32::MIN as f32 - 1.0), i32::MIN);
		assert_eq!(f32_to_i32_sat(i32::MIN as f32 - 0.5), i32::MIN);
		assert_eq!(f32_to_i32_sat(i32::MIN as f32 - 10000.0), i32::MIN);
		assert_eq!(f32_to_i32_sat(i32::MAX as f32), i32::MAX);
		assert_eq!(f32_to_i32_sat(i32::MAX as f32 + 0.5), i32::MAX);
		assert_eq!(f32_to_i32_sat(i32::MAX as f32 + 1.0), i32::MAX);
		assert_eq!(f32_to_i32_sat(i32::MAX as f32 + 10000.0), i32::MAX);
		assert_eq!(f32_to_i32_sat(f32::NAN), 0);
	}

	#[test]
	fn convert_f32_to_i64_sat() {
		assert_eq!(f32_to_i64_sat(0.0), 0);
		assert_eq!(f32_to_i64_sat(i64::MIN as f32), i64::MIN);
		assert_eq!(f32_to_i64_sat(i64::MIN as f32 - 1.0), i64::MIN);
		assert_eq!(f32_to_i64_sat(i64::MIN as f32 - 0.5), i64::MIN);
		assert_eq!(f32_to_i64_sat(i64::MIN as f32 - 10000.0), i64::MIN);
		assert_eq!(f32_to_i64_sat(i64::MAX as f32), i64::MAX);
		assert_eq!(f32_to_i64_sat(i64::MAX as f32 + 0.5), i64::MAX);
		assert_eq!(f32_to_i64_sat(i64::MAX as f32 + 1.0), i64::MAX);
		assert_eq!(f32_to_i64_sat(i64::MAX as f32 + 10000.0), i64::MAX);
		assert_eq!(f32_to_i64_sat(f32::NAN), 0);
	}

	#[test]
	fn convert_f64_to_i32_sat() {
		assert_eq!(f64_to_i32_sat(0.0), 0);
		assert_eq!(f64_to_i32_sat(i32::MIN as f64), i32::MIN);
		assert_eq!(f64_to_i32_sat(i32::MIN as f64 - 0.5), i32::MIN);
		assert_eq!(f64_to_i32_sat(i32::MIN as f64 - 1.0), i32::MIN);
		assert_eq!(f64_to_i32_sat(i32::MIN as f64 - 10000.0), i32::MIN);
		assert_eq!(f64_to_i32_sat(i32::MAX as f64), i32::MAX);
		assert_eq!(f64_to_i32_sat(i32::MAX as f64 + 0.5), i32::MAX);
		assert_eq!(f64_to_i32_sat(i32::MAX as f64 + 1.0), i32::MAX);
		assert_eq!(f64_to_i32_sat(i32::MAX as f64 + 10000.0), i32::MAX);
		assert_eq!(f64_to_i32_sat(f64::NAN), 0);
	}

	#[test]
	fn convert_f64_to_i64_sat() {
		assert_eq!(f64_to_i64_sat(0.0), 0);
		assert_eq!(f64_to_i64_sat(i64::MIN as f64), i64::MIN);
		assert_eq!(f64_to_i64_sat(i64::MIN as f64 - 0.5), i64::MIN);
		assert_eq!(f64_to_i64_sat(i64::MIN as f64 - 1.0), i64::MIN);
		assert_eq!(f64_to_i64_sat(i64::MIN as f64 - 10000.0), i64::MIN);
		assert_eq!(f64_to_i64_sat(i64::MAX as f64), i64::MAX);
		assert_eq!(f64_to_i64_sat(i64::MAX as f64 + 0.5), i64::MAX);
		assert_eq!(f64_to_i64_sat(i64::MAX as f64 + 1.0), i64::MAX);
		assert_eq!(f64_to_i64_sat(i64::MAX as f64 + 10000.0), i64::MAX);
		assert_eq!(f64_to_i64_sat(f64::NAN), 0);
	}

	#[test]
	fn convert_f32_to_u32_sat() {
		assert_eq!(f32_to_u32_sat(-1.0), 0);
		assert_eq!(f32_to_u32_sat(0.5), 0);
		assert_eq!(f32_to_u32_sat(0.0), 0);
		assert_eq!(f32_to_u32_sat(u32::MAX as f32), u32::MAX);
		assert_eq!(f32_to_u32_sat(u32::MAX as f32 + 0.5), u32::MAX);
		assert_eq!(f32_to_u32_sat(u32::MAX as f32 + 1.0), u32::MAX);
		assert_eq!(f32_to_u32_sat(u32::MAX as f32 + 10000.0), u32::MAX);
		assert_eq!(f32_to_u32_sat(f32::NAN), 0);
	}

	#[test]
	fn convert_f32_to_u64_sat() {
		assert_eq!(f32_to_u64_sat(-1.0), 0);
		assert_eq!(f32_to_u64_sat(0.5), 0);
		assert_eq!(f32_to_u64_sat(0.0), 0);
		assert_eq!(f32_to_u64_sat(u64::MAX as f32), u64::MAX);
		assert_eq!(f32_to_u64_sat(u64::MAX as f32 + 0.5), u64::MAX);
		assert_eq!(f32_to_u64_sat(u64::MAX as f32 + 1.0), u64::MAX);
		assert_eq!(f32_to_u64_sat(u64::MAX as f32 + 10000.0), u64::MAX);
		assert_eq!(f32_to_u64_sat(f32::NAN), 0);
	}

	#[test]
	fn convert_f64_to_u32_sat() {
		assert_eq!(f64_to_u32_sat(-1.0), 0);
		assert_eq!(f64_to_u32_sat(0.5), 0);
		assert_eq!(f64_to_u32_sat(0.0), 0);
		assert_eq!(f64_to_u32_sat(u32::MAX as f64), u32::MAX);
		assert_eq!(f64_to_u32_sat(u32::MAX as f64 + 0.5), u32::MAX);
		assert_eq!(f64_to_u32_sat(u32::MAX as f64 + 1.0), u32::MAX);
		assert_eq!(f64_to_u32_sat(u32::MAX as f64 + 10000.0), u32::MAX);
		assert_eq!(f64_to_u32_sat(f64::NAN), 0);
	}

	#[test]
	fn convert_f64_to_u64_sat() {
		assert_eq!(f64_to_u64_sat(-1.0), 0);
		assert_eq!(f64_to_u64_sat(0.5), 0);
		assert_eq!(f64_to_u64_sat(0.0), 0);
		assert_eq!(f64_to_u64_sat(u64::MAX as f64), u64::MAX);
		assert_eq!(f64_to_u64_sat(u64::MAX as f64 + 0.5), u64::MAX);
		assert_eq!(f64_to_u64_sat(u64::MAX as f64 + 1.0), u64::MAX);
		assert_eq!(f64_to_u64_sat(u64::MAX as f64 + 10000.0), u64::MAX);
		assert_eq!(f64_to_u64_sat(f64::NAN), 0);
	}
}
