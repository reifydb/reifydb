// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This code includes parts derived from wain,
// available under the MIT License, with modifications.
//
// Copyright (c) 2020 rhysed
// Licensed under the MIT License (https://opensource.org/licenses/MIT)

use std::fmt;
use std::mem;

use crate::module::{
    ExternalIndex, Instruction, Trap, TrapOverflow, TrapType, TrapUnderflow, Value, ValueType,
};

use crate::execute::Result;

pub type InstructionPointer = isize;
pub type Arity = usize;

pub type BytesPointer = usize;
pub type TypePointer = usize;

#[derive(Clone, Debug, PartialEq)]
pub struct StackPointer {
    bp: BytesPointer,
    tp: TypePointer,
}

pub struct CallFrame {
    pub(crate) ip: InstructionPointer,
    pub(crate) sp: StackPointer,
    pub(crate) instructions: Box<[Instruction]>,
    pub(crate) arity: Arity,
    pub(crate) locals: Box<[Value]>,
}

impl Default for CallFrame {
    fn default() -> Self {
        Self {
            ip: 0,
            sp: StackPointer { bp: 0, tp: 0 },
            instructions: Box::new([]),
            arity: 0,
            locals: Box::new([]),
        }
    }
}

pub(crate) const DEFAULT_MAX_VALUE_STACK: usize = 1024 * 32;

pub struct Stack {
    bytes: Vec<u8>,
    types: Vec<ValueType>,
    pub(crate) frame: CallFrame,
    max_value_stack: usize,
}

/// A trait that defines stack operations for a specific type.
/// This trait is intended to be implemented by types that can be pushed, popped, and peeked on a stack.
///
/// # Type Parameter
/// - `Self`: The type of the value that will be manipulated on the stack.
///
/// # Errors
/// Each method returns a `Result` to handle potential errors:
/// - `Ok`: The operation was successful.
/// - `Trap`: The operation trapped due to an error such as a stack overflow, stack underflow, or type mismatch.
pub trait StackAccess: Sized {
    /// Pushes a value onto the stack and returns the current stack height.
    fn push(stack: &mut Stack, value: Self) -> Result<()>;

    /// Pops a value from the stack.
    fn pop(stack: &mut Stack) -> Result<Self> {
        let value = Self::peek(stack)?;
        stack.pop_bytes(size_of::<Self>())?;
        Ok(value)
    }

    /// Peeks at the top value of the stack without removing it.
    fn peek(stack: &Stack) -> Result<Self>;
}

impl Default for Stack {
    fn default() -> Self {
        Self {
            bytes: Default::default(),
            types: Default::default(),
            frame: Default::default(),
            max_value_stack: DEFAULT_MAX_VALUE_STACK,
        }
    }
}

impl Stack {
    pub fn with_max_size(max_value_stack: usize) -> Self {
        Self {
            bytes: Default::default(),
            types: Default::default(),
            frame: Default::default(),
            max_value_stack,
        }
    }
}

impl StackAccess for i32 {
    fn push(stack: &mut Stack, value: i32) -> Result<()> {
        stack.push_bytes(&value.to_le_bytes(), ValueType::I32)
    }

    fn peek(stack: &Stack) -> Result<i32> {
        stack.expect_type(ValueType::I32)?;
        Ok(i32::from_le_bytes(stack.peek_bytes(size_of::<i32>())?))
    }
}

impl StackAccess for u32 {
    fn push(stack: &mut Stack, value: Self) -> Result<()> {
        stack.push_bytes(&value.to_le_bytes(), ValueType::I32)
    }

    fn peek(stack: &Stack) -> Result<Self> {
        stack.expect_type(ValueType::I32)?;
        Ok(u32::from_le_bytes(stack.peek_bytes(size_of::<u32>())?))
    }
}

impl StackAccess for i64 {
    fn push(stack: &mut Stack, value: i64) -> Result<()> {
        stack.push_bytes(&value.to_le_bytes(), ValueType::I64)
    }

    fn peek(stack: &Stack) -> Result<i64> {
        stack.expect_type(ValueType::I64)?;
        Ok(i64::from_le_bytes(stack.peek_bytes(size_of::<i64>())?))
    }
}

impl StackAccess for u64 {
    fn push(stack: &mut Stack, value: u64) -> Result<()> {
        stack.push_bytes(&value.to_le_bytes(), ValueType::I64)
    }

    fn peek(stack: &Stack) -> Result<u64> {
        stack.expect_type(ValueType::I64)?;
        Ok(u64::from_le_bytes(stack.peek_bytes(size_of::<u64>())?))
    }
}

impl StackAccess for f32 {
    fn push(stack: &mut Stack, value: f32) -> Result<()> {
        stack.push_bytes(&value.to_le_bytes(), ValueType::F32)
    }

    fn peek(stack: &Stack) -> Result<f32> {
        stack.expect_type(ValueType::F32)?;
        Ok(f32::from_le_bytes(stack.peek_bytes(size_of::<f32>())?))
    }
}

impl StackAccess for f64 {
    fn push(stack: &mut Stack, value: f64) -> Result<()> {
        stack.push_bytes(&value.to_le_bytes(), ValueType::F64)
    }

    fn peek(stack: &Stack) -> Result<f64> {
        stack.expect_type(ValueType::F64)?;
        Ok(f64::from_le_bytes(stack.peek_bytes(size_of::<f64>())?))
    }
}

impl StackAccess for ExternalIndex {
    fn push(stack: &mut Stack, value: Self) -> Result<()> {
        stack.push_bytes(&value.0.to_le_bytes(), ValueType::RefExtern)
    }

    fn peek(stack: &Stack) -> Result<Self> {
        stack.expect_type(ValueType::RefExtern)?;
        Ok(ExternalIndex(u32::from_le_bytes(
            stack.peek_bytes(size_of::<u32>())?,
        )))
    }
}

impl StackAccess for Value {
    fn push(stack: &mut Stack, v: Self) -> Result<()> {
        match v {
            Value::I32(v) => StackAccess::push(stack, v),
            Value::I64(v) => StackAccess::push(stack, v),
            Value::F32(v) => StackAccess::push(stack, v),
            Value::F64(v) => StackAccess::push(stack, v),
            Value::RefFunc(_idx) => todo!(),
            Value::RefExtern(idx) => StackAccess::push(stack, idx),
            Value::RefNull(_ty) => todo!(),
        }
    }

    fn pop(stack: &mut Stack) -> Result<Self> {
        match stack.peek_type()? {
            ValueType::I32 => StackAccess::pop(stack).map(|v| Value::I32(v)),
            ValueType::I64 => StackAccess::pop(stack).map(|v| Value::I64(v)),
            ValueType::F32 => StackAccess::pop(stack).map(|v| Value::F32(v)),
            ValueType::F64 => StackAccess::pop(stack).map(|v| Value::F64(v)),
            ValueType::RefExtern => StackAccess::pop(stack).map(|idx| Value::RefExtern(idx)),
            ValueType::RefFunc => todo!(),
        }
    }

    fn peek(stack: &Stack) -> Result<Self> {
        match stack.peek_type()? {
            ValueType::I32 => StackAccess::peek(stack).map(|v| Value::I32(v)),
            ValueType::I64 => StackAccess::peek(stack).map(|v| Value::I64(v)),
            ValueType::F32 => StackAccess::peek(stack).map(|v| Value::F32(v)),
            ValueType::F64 => StackAccess::peek(stack).map(|v| Value::F64(v)),
            ValueType::RefExtern => StackAccess::peek(stack).map(|idx| Value::RefExtern(idx)),
            ValueType::RefFunc => todo!(),
        }
    }
}

impl Stack {
    /// Pushes a value onto the stack.
    pub fn push<V: StackAccess>(&mut self, v: V) -> Result<()> {
        StackAccess::push(self, v)
    }

    /// Peeks at the top value of the stack without removing it.
    pub fn peek<V: StackAccess>(&mut self) -> Result<V> {
        StackAccess::peek(self)
    }

    /// Pops the top value off the stack.
    pub fn pop<V: StackAccess>(&mut self) -> Result<V> {
        StackAccess::pop(self)
    }

    /// Pushes raw bytes onto the stack along with a specified value type.
    fn push_bytes(&mut self, bytes: &[u8], vt: ValueType) -> Result<()> {
        if self.types.len() + 1 > self.max_value_stack {
            return Err(Trap::Overflow(TrapOverflow::Stack));
        }
        self.bytes.extend_from_slice(bytes);
        self.types.push(vt);
        Ok(())
    }

    /// Pops raw bytes off the stack based on the specified size.
    fn pop_bytes(&mut self, s: usize) -> Result<()> {
        self.bytes.truncate(self.bytes.len() - s);
        self.types
            .pop()
            .ok_or(Trap::Underflow(TrapUnderflow::Stack))?;
        Ok(())
    }

    /// Peeks at a specific number of bytes from the top of the stack.
    fn peek_bytes<'a, T>(&'a self, s: usize) -> Result<T>
    where
        T: TryFrom<&'a [u8]>,
        T::Error: fmt::Debug,
    {
        Ok(self.bytes[self.bytes.len() - s..]
            .try_into()
            .map_err(|_| Trap::Underflow(TrapUnderflow::Stack))?)
    }

    /// Checks if the top value on the stack matches the expected type.
    fn expect_type(&self, expected: ValueType) -> Result<()> {
        let got = self.peek_type()?.clone();
        if got != expected {
            Err(Trap::Type(TrapType::MismatchValueType(expected, got)))
        } else {
            Ok(())
        }
    }

    /// Peeks at the type of the top value on the stack.
    fn peek_type(&self) -> Result<&ValueType> {
        self.types
            .last()
            .ok_or(Trap::Underflow(TrapUnderflow::Stack))
    }

    /// Replaces the top type on the stack with the given `ValueType`.
    pub(crate) fn replace_type(&mut self, vt: ValueType) -> Result<()> {
        if self.pointer().tp == 0 {
            return Err(Trap::Underflow(TrapUnderflow::Stack));
        }
        let index = self.types.len() - 1;
        self.types[index] = vt;
        Ok(())
    }

    /// Returns the current pointer of the stack.
    pub fn pointer(&self) -> StackPointer {
        StackPointer {
            bp: self.bytes.len(),
            tp: self.types.len(),
        }
    }

    /// Replaces the current call frame with a new one and returns the old frame.
    pub(crate) fn replace_frame(&mut self, frame: CallFrame) -> CallFrame {
        mem::replace(&mut self.frame, frame)
    }

    /// Restores a previous `CallFrame` as the current frame without returning the old one.
    pub(crate) fn restore_frame(&mut self, frame: CallFrame) {
        let _ = mem::replace(&mut self.frame, frame);
    }

    /// Reverts the stack to a previous state, preserving the result types.
    pub fn revert(
        &mut self,
        sp: StackPointer,
        result_types: impl AsRef<[ValueType]>,
    ) -> Result<()> {
        let result_types = result_types.as_ref();
        let mut results = Vec::with_capacity(result_types.len());

        for rt in result_types.iter().rev() {
            self.expect_type(rt.clone())?;
            let v: Value = self.pop()?;
            results.insert(0, v);
        }

        self.bytes.truncate(sp.bp);
        self.types.truncate(sp.tp);

        for value in results {
            self.push(value)?
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i32_primitive() {
        let mut ti = Stack::default();
        ti.push(0i32).unwrap();
        assert_eq!(ti.peek::<i32>().unwrap(), 0);

        ti.push(1i32).unwrap();
        ti.push(-1i32).unwrap();
        ti.push(i32::MAX).unwrap();
        ti.push(i32::MIN).unwrap();

        assert_eq!(ti.peek::<i32>().unwrap(), i32::MIN);
        assert_eq!(ti.pop::<i32>().unwrap(), i32::MIN);
        assert_eq!(ti.peek::<i32>().unwrap(), i32::MAX);
        assert_eq!(ti.pop::<i32>().unwrap(), i32::MAX);
        assert_eq!(ti.peek::<i32>().unwrap(), -1);
        assert_eq!(ti.pop::<i32>().unwrap(), -1);
        assert_eq!(ti.peek::<i32>().unwrap(), 1);
        assert_eq!(ti.pop::<i32>().unwrap(), 1);
        assert_eq!(ti.peek::<i32>().unwrap(), 0);
        assert_eq!(ti.pop::<i32>().unwrap(), 0);
    }

    #[test]
    fn i32_value() {
        let mut ti = Stack::default();
        ti.push(Value::I32(0i32)).unwrap();
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(0));

        ti.push(Value::I32(1i32)).unwrap();
        ti.push(Value::I32(-1i32)).unwrap();
        ti.push(Value::I32(i32::MAX)).unwrap();
        ti.push(Value::I32(i32::MIN)).unwrap();

        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(i32::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(i32::MIN));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(i32::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(i32::MAX));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(-1));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(-1));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(1));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(1));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(0));
    }

    #[test]
    fn i32_mixed() {
        let mut ti = Stack::default();
        ti.push(Value::I32(i32::MAX)).unwrap();
        ti.push(i32::MAX).unwrap();
        ti.push(Value::I32(i32::MIN)).unwrap();
        ti.push(i32::MIN).unwrap();

        assert_eq!(ti.peek::<i32>().unwrap(), i32::MIN);
        assert_eq!(ti.pop::<i32>().unwrap(), i32::MIN);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(i32::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(i32::MIN));
        assert_eq!(ti.peek::<i32>().unwrap(), i32::MAX);
        assert_eq!(ti.pop::<i32>().unwrap(), i32::MAX);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I32(i32::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I32(i32::MAX));
    }

    #[test]
    fn i64_primitive() {
        let mut ti = Stack::default();
        ti.push(0i64).unwrap();
        assert_eq!(ti.peek::<i64>().unwrap(), 0);

        ti.push(1i64).unwrap();
        ti.push(-1i64).unwrap();
        ti.push(i64::MAX).unwrap();
        ti.push(i64::MIN).unwrap();

        assert_eq!(ti.peek::<i64>().unwrap(), i64::MIN);
        assert_eq!(ti.pop::<i64>().unwrap(), i64::MIN);
        assert_eq!(ti.peek::<i64>().unwrap(), i64::MAX);
        assert_eq!(ti.pop::<i64>().unwrap(), i64::MAX);
        assert_eq!(ti.peek::<i64>().unwrap(), -1);
        assert_eq!(ti.pop::<i64>().unwrap(), -1);
        assert_eq!(ti.peek::<i64>().unwrap(), 1);
        assert_eq!(ti.pop::<i64>().unwrap(), 1);
        assert_eq!(ti.peek::<i64>().unwrap(), 0);
        assert_eq!(ti.pop::<i64>().unwrap(), 0);
    }

    #[test]
    fn i64_value() {
        let mut ti = Stack::default();
        ti.push(Value::I64(0i64)).unwrap();
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(0));

        ti.push(Value::I64(1i64)).unwrap();
        ti.push(Value::I64(-1i64)).unwrap();
        ti.push(Value::I64(i64::MAX)).unwrap();
        ti.push(Value::I64(i64::MIN)).unwrap();

        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(i64::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(i64::MIN));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(i64::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(i64::MAX));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(-1));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(-1));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(1));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(1));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(0));
    }

    #[test]
    fn i64_mixed() {
        let mut ti = Stack::default();
        ti.push(Value::I64(i64::MAX)).unwrap();
        ti.push(i64::MAX).unwrap();
        ti.push(Value::I64(i64::MIN)).unwrap();
        ti.push(i64::MIN).unwrap();

        assert_eq!(ti.peek::<i64>().unwrap(), i64::MIN);
        assert_eq!(ti.pop::<i64>().unwrap(), i64::MIN);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(i64::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(i64::MIN));
        assert_eq!(ti.peek::<i64>().unwrap(), i64::MAX);
        assert_eq!(ti.pop::<i64>().unwrap(), i64::MAX);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::I64(i64::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::I64(i64::MAX));
    }

    #[test]
    fn f32_primitive() {
        let mut ti = Stack::default();
        ti.push(0f32).unwrap();
        assert_eq!(ti.peek::<f32>().unwrap(), 0.0);

        ti.push(1f32).unwrap();
        ti.push(-1f32).unwrap();
        ti.push(f32::MAX).unwrap();
        ti.push(f32::MIN).unwrap();

        assert_eq!(ti.peek::<f32>().unwrap(), f32::MIN);
        assert_eq!(ti.pop::<f32>().unwrap(), f32::MIN);
        assert_eq!(ti.peek::<f32>().unwrap(), f32::MAX);
        assert_eq!(ti.pop::<f32>().unwrap(), f32::MAX);
        assert_eq!(ti.peek::<f32>().unwrap(), -1.0);
        assert_eq!(ti.pop::<f32>().unwrap(), -1.0);
        assert_eq!(ti.peek::<f32>().unwrap(), 1.0);
        assert_eq!(ti.pop::<f32>().unwrap(), 1.0);
        assert_eq!(ti.peek::<f32>().unwrap(), 0.0);
        assert_eq!(ti.pop::<f32>().unwrap(), 0.0);
    }

    #[test]
    fn f32_value() {
        let mut ti = Stack::default();
        ti.push(Value::F32(0f32)).unwrap();
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(0.0));

        ti.push(Value::F32(1.0f32)).unwrap();
        ti.push(Value::F32(-1.0f32)).unwrap();
        ti.push(Value::F32(f32::MAX)).unwrap();
        ti.push(Value::F32(f32::MIN)).unwrap();

        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(f32::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(f32::MIN));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(f32::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(f32::MAX));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(-1.0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(-1.0));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(1.0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(1.0));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(0.0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(0.0));
    }

    #[test]
    fn f32_mixed() {
        let mut ti = Stack::default();
        ti.push(Value::F32(f32::MAX)).unwrap();
        ti.push(f32::MAX).unwrap();
        ti.push(Value::F32(f32::MIN)).unwrap();
        ti.push(f32::MIN).unwrap();

        assert_eq!(ti.peek::<f32>().unwrap(), f32::MIN);
        assert_eq!(ti.pop::<f32>().unwrap(), f32::MIN);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(f32::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(f32::MIN));
        assert_eq!(ti.peek::<f32>().unwrap(), f32::MAX);
        assert_eq!(ti.pop::<f32>().unwrap(), f32::MAX);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F32(f32::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F32(f32::MAX));
    }

    #[test]
    fn f64_primitive() {
        let mut ti = Stack::default();
        ti.push(0f64).unwrap();
        assert_eq!(ti.peek::<f64>().unwrap(), 0.0);

        ti.push(1f64).unwrap();
        ti.push(-1f64).unwrap();
        ti.push(f64::MAX).unwrap();
        ti.push(f64::MIN).unwrap();

        assert_eq!(ti.peek::<f64>().unwrap(), f64::MIN);
        assert_eq!(ti.pop::<f64>().unwrap(), f64::MIN);
        assert_eq!(ti.peek::<f64>().unwrap(), f64::MAX);
        assert_eq!(ti.pop::<f64>().unwrap(), f64::MAX);
        assert_eq!(ti.peek::<f64>().unwrap(), -1.0);
        assert_eq!(ti.pop::<f64>().unwrap(), -1.0);
        assert_eq!(ti.peek::<f64>().unwrap(), 1.0);
        assert_eq!(ti.pop::<f64>().unwrap(), 1.0);
        assert_eq!(ti.peek::<f64>().unwrap(), 0.0);
        assert_eq!(ti.pop::<f64>().unwrap(), 0.0);
    }

    #[test]
    fn f64_value() {
        let mut ti = Stack::default();
        ti.push(Value::F64(0f64)).unwrap();
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(0.0));

        ti.push(Value::F64(1.0f64)).unwrap();
        ti.push(Value::F64(-1.0f64)).unwrap();
        ti.push(Value::F64(f64::MAX)).unwrap();
        ti.push(Value::F64(f64::MIN)).unwrap();

        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(f64::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(f64::MIN));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(f64::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(f64::MAX));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(-1.0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(-1.0));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(1.0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(1.0));
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(0.0));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(0.0));
    }

    #[test]
    fn f64_mixed() {
        let mut ti = Stack::default();
        ti.push(Value::F64(f64::MAX)).unwrap();
        ti.push(f64::MAX).unwrap();
        ti.push(Value::F64(f64::MIN)).unwrap();
        ti.push(f64::MIN).unwrap();

        assert_eq!(ti.peek::<f64>().unwrap(), f64::MIN);
        assert_eq!(ti.pop::<f64>().unwrap(), f64::MIN);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(f64::MIN));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(f64::MIN));
        assert_eq!(ti.peek::<f64>().unwrap(), f64::MAX);
        assert_eq!(ti.pop::<f64>().unwrap(), f64::MAX);
        assert_eq!(ti.peek::<Value>().unwrap(), Value::F64(f64::MAX));
        assert_eq!(ti.pop::<Value>().unwrap(), Value::F64(f64::MAX));
    }

    #[test]
    fn type_mismatch_on_pop() {
        let mut ti = Stack::default();
        ti.push(42i32).unwrap();

        let result: Result<i64> = ti.pop();
        assert_eq!(
            result,
            Err(Trap::Type(TrapType::MismatchValueType(
                ValueType::I64,
                ValueType::I32
            )))
        );
    }

    #[test]
    fn type_mismatch_on_pop_value() {
        let mut ti = Stack::default();
        ti.push(Value::I32(23)).unwrap();
        let result: Result<i64> = ti.pop();
        assert_eq!(
            result,
            Err(Trap::Type(TrapType::MismatchValueType(
                ValueType::I64,
                ValueType::I32
            )))
        );
    }

    #[test]
    fn type_mismatch_on_peek() {
        let mut ti = Stack::default();
        ti.push(42i32).unwrap();
        let result: Result<i64> = ti.peek();
        assert_eq!(
            result,
            Err(Trap::Type(TrapType::MismatchValueType(
                ValueType::I64,
                ValueType::I32
            )))
        );
    }

    #[test]
    fn type_mismatch_on_peek_value() {
        let mut ti = Stack::default();
        ti.push(Value::I32(23)).unwrap();
        let result: Result<i64> = ti.peek();
        assert_eq!(
            result,
            Err(Trap::Type(TrapType::MismatchValueType(
                ValueType::I64,
                ValueType::I32
            )))
        );
    }

    #[test]
    fn stack_underflow_on_pop() {
        let mut ti = Stack::default();
        let result: Result<i32> = ti.pop();
        assert_eq!(result, Err(Trap::Underflow(TrapUnderflow::Stack)));
    }

    #[test]
    fn stack_underflow_on_peek() {
        let mut ti = Stack::default();
        let result: Result<i32> = ti.peek();
        assert_eq!(result, Err(Trap::Underflow(TrapUnderflow::Stack)));
    }

    #[test]
    fn stack_overflow() {
        let mut ti = Stack::default();
        for i in 0..DEFAULT_MAX_VALUE_STACK {
            ti.push(i as i32).unwrap()
        }

        let result: Result<()> = ti.push(42i32);
        assert_eq!(result, Err(Trap::Overflow(TrapOverflow::Stack)));
    }

    #[test]
    fn pointer() {
        let mut ti = Stack::default();
        assert_eq!(ti.pointer(), StackPointer { bp: 0, tp: 0 });

        ti.push(23i32).unwrap();
        assert_eq!(ti.pointer(), StackPointer { bp: 4, tp: 1 });

        ti.pop::<i32>().unwrap();
        assert_eq!(ti.pointer(), StackPointer { bp: 0, tp: 0 });

        let _ = ti.pop::<i32>();
        let _ = ti.pop::<i32>();

        assert_eq!(ti.pointer(), StackPointer { bp: 0, tp: 0 });
    }

    #[test]
    fn replace_type_success() {
        let mut ti = Stack::default();
        ti.push(Value::I32(32)).unwrap();

        assert!(ti.replace_type(ValueType::F32).is_ok());
        assert_eq!(ti.types.last().unwrap(), &ValueType::F32);
    }

    #[test]
    fn replace_type_with_multiple_elements() {
        let mut ti = Stack::default();
        ti.push(Value::I32(32)).unwrap();
        ti.push(Value::I64(64)).unwrap();

        assert!(ti.replace_type(ValueType::F64).is_ok());
        assert_eq!(ti.types.last().unwrap(), &ValueType::F64);
        assert_eq!(ti.types.len(), 2);
    }

    #[test]
    fn replace_type_on_empty_stack() {
        let mut ti = Stack::default();

        let result = ti.replace_type(ValueType::F32);
        assert!(result.is_err());
    }

    #[test]
    fn revert_without_result_type() {
        let mut ti = Stack::default();

        let ground = ti.pointer();
        ti.push(10i32).unwrap();

        let middle = ti.pointer();
        ti.push(20i32).unwrap();

        let end = ti.pointer();

        // does nothing
        ti.revert(end, []).unwrap();

        ti.revert(middle.clone(), []).unwrap();
        assert_eq!(ti.pointer(), middle);
        assert_eq!(ti.peek::<i32>().unwrap(), 10i32);

        ti.revert(ground.clone(), []).unwrap();
        assert_eq!(ti.pointer(), ground);
        assert!(ti.peek::<i32>().is_err());
    }

    #[test]
    fn revert_with_result_type() {
        let mut ti = Stack::default();

        let ground = ti.pointer();
        ti.push(10i32).unwrap();

        let middle = ti.pointer();
        ti.push(20i32).unwrap();

        let end = ti.pointer();

        // does nothing
        ti.revert(end, [ValueType::I32]).unwrap();

        // pops value and push it again
        ti.revert(middle.clone(), [ValueType::I32]).unwrap();
        assert_eq!(ti.pointer(), StackPointer { bp: 8, tp: 2 });
        assert_eq!(ti.peek::<i32>().unwrap(), 20i32);

        ti.revert(ground.clone(), [ValueType::I32]).unwrap();
        assert_eq!(ti.pointer(), StackPointer { bp: 4, tp: 1 });
        assert_eq!(ti.peek::<i32>().unwrap(), 20i32);
    }

    #[test]
    fn revert_with_multiple_result_types() {
        let mut ti = Stack::default();

        let ground = ti.pointer();
        ti.push(10i32).unwrap();
        ti.push(20i32).unwrap();
        ti.push(30i32).unwrap();

        let end = ti.pointer();

        // does nothing
        ti.revert(end, [ValueType::I32, ValueType::I32]).unwrap();

        ti.revert(ground.clone(), [ValueType::I32, ValueType::I32])
            .unwrap();
        assert_eq!(ti.pointer(), StackPointer { bp: 8, tp: 2 });
        assert_eq!(ti.peek::<i32>().unwrap(), 30i32);
    }

    #[test]
    fn revert_with_type_mismatch() {
        let mut ti = Stack::default();
        ti.push(10i32).unwrap();

        let result = ti.revert(ti.pointer(), [ValueType::F64]);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "expected type F64, got I32".to_string()
        );
    }
}
