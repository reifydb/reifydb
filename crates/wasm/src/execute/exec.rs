// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{
	config::WasmConfig,
	execute::{
		Result,
		instruction::{ExecInstruction, ExecStatus},
		stack::{CallFrame, Stack, StackAccess},
		state::State,
	},
	module::{
		ExportData, Function, FunctionExternal, FunctionIndex, FunctionLocal, FunctionTypeIndex, GlobalIndex,
		LocalIndex, MemoryArgument, MemoryReader, MemoryWriter, TableIndex, Trap, TrapNotFound, TrapType,
		Value, ValueType,
	},
};

// ---------------------------------------------------------------------------
// HostFunction
// ---------------------------------------------------------------------------

pub type HostFn = Arc<dyn Fn(&mut Exec) -> Result<()> + Send + Sync>;

pub struct HostFunctionRegistry {
	pub(crate) functions: Vec<(String, String, HostFn)>,
}

impl Default for HostFunctionRegistry {
	fn default() -> Self {
		Self {
			functions: Vec::new(),
		}
	}
}

impl HostFunctionRegistry {
	pub fn register(
		&mut self,
		module: impl Into<String>,
		name: impl Into<String>,
		f: impl Fn(&mut Exec) -> Result<()> + Send + Sync + 'static,
	) {
		self.functions.push((module.into(), name.into(), Arc::new(f)));
	}

	pub fn resolve(&self, module: &str, name: &str) -> Option<HostFn> {
		self.functions.iter().find(|(m, n, _)| m == module && n == name).map(|(_, _, f)| f.clone())
	}
}

// ---------------------------------------------------------------------------
// Exec
// ---------------------------------------------------------------------------

pub struct Exec {
	pub(crate) state: State,
	pub(crate) stack: Stack,
	pub(crate) config: WasmConfig,
	pub(crate) fuel: u64,
	pub(crate) call_depth: u32,
	pub(crate) host_functions: HostFunctionRegistry,
	pub(crate) imported_functions: Vec<(String, String, Arc<Function>)>,
}

impl Exec {
	pub fn new(state: State) -> Self {
		let config = WasmConfig::default();
		let fuel = config.max_instructions;
		Self {
			state,
			stack: Stack::with_max_size(config.max_stack_size),
			fuel,
			call_depth: 0,
			host_functions: HostFunctionRegistry::default(),
			imported_functions: vec![],
			config,
		}
	}

	pub fn with_config(state: State, config: WasmConfig) -> Self {
		let fuel = config.max_instructions;
		Self {
			state,
			stack: Stack::with_max_size(config.max_stack_size),
			fuel,
			call_depth: 0,
			host_functions: HostFunctionRegistry::default(),
			imported_functions: vec![],
			config,
		}
	}

	pub fn register_host_function(
		&mut self,
		module: impl Into<String>,
		name: impl Into<String>,
		f: impl Fn(&mut Exec) -> Result<()> + Send + Sync + 'static,
	) {
		self.host_functions.register(module, name, f);
	}

	/// Pop a typed value from the stack. Available for host function implementations.
	pub fn pop<V: StackAccess>(&mut self) -> Result<V> {
		self.stack.pop()
	}

	/// Push a typed value onto the stack. Available for host function implementations.
	pub fn push<V: StackAccess>(&mut self, value: V) -> Result<()> {
		self.stack.push(value)
	}

	pub(crate) fn consume_fuel(&mut self) -> Result<()> {
		if self.fuel == 0 {
			return Err(Trap::OutOfFuel);
		}
		self.fuel -= 1;
		Ok(())
	}

	pub fn invoke(
		&mut self,
		name: impl Into<String>,
		args: impl AsRef<[Value]>,
	) -> std::result::Result<Box<[Value]>, Trap> {
		self.call_depth = 0;
		let name = name.into();

		let idx = match self
			.state
			.export(name.clone())
			.or_else(|_| Err(Trap::NotFound(TrapNotFound::ExportedFunction(name))))?
			.data()
		{
			ExportData::Function(idx) => *idx as usize,
			_ => unreachable!("can only invoke functions"),
		};

		for arg in args.as_ref() {
			self.stack.push(arg.clone())?;
		}

		let function = self.state.function(idx as FunctionIndex).unwrap();
		match &*function {
			Function::Local(func_inst) => {
				let result_count = func_inst.result_count();
				let previous_frame = self.push_frame(func_inst)?;
				for instruction in func_inst.instructions() {
					self.consume_fuel()?;
					match instruction.execute(self)? {
						ExecStatus::Continue => {}
						ExecStatus::Break(_) | ExecStatus::Return => break,
					}
				}
				self.restore_frame(previous_frame);

				let mut result = vec![];
				for _ in 0..result_count {
					let value = self.stack.pop()?;
					result.insert(0, value);
				}
				Ok(result.into())
			}
			Function::External(external) => {
				let result_count = external.function_type.results.len();
				self.call_external(external)?;
				let mut result = vec![];
				for _ in 0..result_count {
					let value = self.stack.pop()?;
					result.insert(0, value);
				}
				Ok(result.into())
			}
		}
	}

	pub fn call(&mut self, idx: &FunctionIndex) -> Result<()> {
		let func = self.function(*idx)?;
		let func = func.as_ref();

		match func {
			Function::Local(f) => self.call_local(f),
			Function::External(f) => self.call_external(f),
		}
	}

	pub fn call_local(&mut self, f: &FunctionLocal) -> Result<()> {
		let previous_frame = self.push_frame(f)?;

		for instruction in f.instructions() {
			self.consume_fuel()?;
			match instruction.execute(self)? {
				ExecStatus::Continue => {}
				ExecStatus::Break(_) | ExecStatus::Return => break,
			}
		}

		self.restore_frame(previous_frame);
		Ok(())
	}

	pub fn call_external(&mut self, f: &FunctionExternal) -> Result<()> {
		// Try host functions first
		if let Some(host_fn) = self.host_functions.resolve(&f.module, &f.function_name) {
			return host_fn(self);
		}

		// Try imported WASM functions from registered modules
		let imported = self
			.imported_functions
			.iter()
			.find(|(m, n, _)| m == &f.module && n == &f.function_name)
			.map(|(_, _, func)| func.clone());

		if let Some(func) = imported {
			match &*func {
				Function::Local(local) => {
					let previous_frame = self.push_frame(local)?;
					for instruction in local.instructions() {
						self.consume_fuel()?;
						match instruction.execute(self)? {
							ExecStatus::Continue => {}
							ExecStatus::Break(_) | ExecStatus::Return => break,
						}
					}
					self.restore_frame(previous_frame);
					return Ok(());
				}
				Function::External(ef) => {
					return self.call_external(ef);
				}
			}
		}

		Err(Trap::UnresolvedHostFunction(f.module.clone(), f.function_name.clone()))
	}

	pub fn call_indirect(&mut self, type_idx: FunctionTypeIndex, table_idx: TableIndex) -> Result<()> {
		let expected = self.state.function_type(type_idx)?;
		let element_idx = self.stack.pop::<u32>()?;

		let table_value = self
			.state
			.table_at(table_idx, element_idx as TableIndex)
			.map_err(|_| Trap::UndefinedElement)?;

		match table_value {
			Value::RefNull(_) => {
				return Err(Trap::UninitializedElement);
			}
			Value::RefFunc(function_idx) => {
				let function = self.state.function(function_idx)?;

				match &*function {
					Function::Local(local) => {
						let actual = &local.function_type;
						if expected.params != actual.params
							|| expected.results != actual.results
						{
							return Err(Trap::Type(TrapType::MismatchIndirectCallType(
								expected,
								actual.clone(),
							)));
						}

						let previous_frame = self.push_frame(local)?;
						for instruction in local.instructions() {
							self.consume_fuel()?;
							match instruction.execute(self)? {
								ExecStatus::Continue => {}
								ExecStatus::Break(_) | ExecStatus::Return => break,
							}
						}
						self.restore_frame(previous_frame);
					}
					Function::External(f) => {
						self.call_external(f)?;
					}
				}
			}
			value => {
				return Err(Trap::Type(TrapType::MismatchValueType(
					ValueType::RefFunc,
					ValueType::from(value),
				)));
			}
		}
		Ok(())
	}

	pub fn local_get(&mut self, idx: LocalIndex) -> Result<()> {
		let Some(value) = self.stack.frame.locals.get(idx as usize) else {
			panic!("not found local");
		};
		self.stack.push(value.clone())?;
		Ok(())
	}

	pub fn local_set(&mut self, idx: LocalIndex) -> Result<()> {
		let value = self.stack.pop()?;
		self.stack.frame.locals[idx as usize] = value;
		Ok(())
	}

	pub fn local_tee(&mut self, idx: LocalIndex) -> Result<()> {
		let value = self.stack.pop::<Value>()?;
		self.stack.push(value.clone())?;
		self.stack.frame.locals[idx as usize] = value;
		Ok(())
	}

	pub fn global_get(&mut self, idx: GlobalIndex) -> Result<()> {
		let value = self.state.global.get(idx)?;
		self.stack.push(value)
	}

	pub fn global_set(&mut self, idx: GlobalIndex) -> Result<()> {
		let value = self.stack.pop::<Value>()?;
		self.state.global.set(idx, value)
	}

	pub(crate) fn load<T, R>(&mut self, mem: &MemoryArgument) -> Result<()>
	where
		T: StackAccess,
		R: MemoryReader + Into<T>,
	{
		let memory = self.state.memory(0)?;
		let idx = self.stack.pop::<u32>()?.saturating_add(mem.offset);

		let value: R = memory.read(idx as usize)?;
		self.stack.push(value.into())
	}

	pub(crate) fn store<W>(&mut self, mem: &MemoryArgument, value: W) -> Result<()>
	where
		W: MemoryWriter,
	{
		let memory = self.state.memory_mut(0)?;
		let idx = self.stack.pop::<u32>()? + mem.offset;
		memory.write(idx as usize, value)
	}

	pub fn memory_grow(&mut self, pages: u32) -> Result<u32> {
		let max_pages = self.config.max_memory_pages;
		let memory = self.state.memory_mut(0)?;
		memory.grow_checked(pages, max_pages)
	}

	pub fn function(&self, idx: FunctionIndex) -> std::result::Result<Arc<Function>, Trap> {
		self.state.function(idx)
	}

	pub(crate) fn unary<T, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		F: FnOnce(T) -> T,
	{
		let result = op(self.stack.pop()?);
		self.stack.push(result)
	}

	pub(crate) fn unary_test<T, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		F: FnOnce(T) -> bool,
	{
		let result = op(self.stack.pop()?);
		self.stack.push(if result {
			Value::I32(1)
		} else {
			Value::I32(0)
		})
	}

	pub(crate) fn unary_map<T, U, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		U: StackAccess,
		F: FnOnce(T) -> U,
	{
		let result = op(self.stack.pop()?);
		self.stack.push(result)
	}

	pub(crate) fn unary_trap<T, U, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		U: StackAccess,
		F: FnOnce(T) -> Result<U>,
	{
		let result = op(self.stack.pop()?)?;
		self.stack.push(result)
	}

	pub(crate) fn binary<T, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		F: FnOnce(T, T) -> T,
	{
		let r = self.stack.pop()?;
		let l = self.stack.pop()?;
		self.stack.push(op(l, r))
	}

	pub(crate) fn binary_trap<T, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		F: FnOnce(T, T) -> Result<T>,
	{
		let r = self.stack.pop()?;
		let l = self.stack.pop()?;
		self.stack.push(op(l, r)?)
	}

	pub(crate) fn binary_test<T, F>(&mut self, op: F) -> Result<()>
	where
		T: StackAccess,
		F: FnOnce(T, T) -> bool,
	{
		let r = self.stack.pop()?;
		let l = self.stack.pop()?;
		let result = op(l, r);
		self.stack.push(if result {
			Value::I32(1)
		} else {
			Value::I32(0)
		})
	}

	pub(crate) fn push_frame(&mut self, func: &FunctionLocal) -> Result<CallFrame> {
		self.call_depth += 1;
		if self.call_depth > self.config.max_call_depth {
			self.call_depth -= 1;
			return Err(Trap::CallDepthExceeded);
		}

		let mut locals = Vec::with_capacity(func.parameter_count());

		for _ in func.parameters().iter() {
			locals.insert(0, self.stack.pop()?);
		}

		for local in func.locals().iter() {
			match local {
				ValueType::I32 => locals.push(Value::I32(0)),
				ValueType::I64 => locals.push(Value::I64(0)),
				ValueType::F32 => locals.push(Value::F32(0.0)),
				ValueType::F64 => locals.push(Value::F64(0.0)),
				ValueType::RefFunc => locals.push(Value::RefNull(ValueType::RefFunc)),
				ValueType::RefExtern => locals.push(Value::RefNull(ValueType::RefExtern)),
			}
		}

		let arity = func.result_count();

		let frame = CallFrame {
			ip: -1,
			sp: self.stack.pointer(),
			instructions: func.instructions().clone(),
			arity,
			locals: locals.into(),
		};

		Ok(self.stack.replace_frame(frame))
	}

	pub(crate) fn restore_frame(&mut self, frame: CallFrame) {
		self.call_depth = self.call_depth.saturating_sub(1);
		let _ = self.stack.restore_frame(frame);
	}
}
