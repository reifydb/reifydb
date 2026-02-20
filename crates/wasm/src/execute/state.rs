// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::module::{
    Export, Function, FunctionIndex, FunctionType, FunctionTypeIndex, Global,
    GlobalIndex, Memory, MemoryIndex, Module, Table, TableElementIndex, TableIndex, Trap,
    TrapNotFound, Value,
};

use crate::execute::Result;

// ---------------------------------------------------------------------------
// StateError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum StateError {
    NotFoundFunction(String),
    NotFoundMemory(MemoryIndex),
    NotFoundModule(String),
    NotFoundTypes,
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateError::NotFoundFunction(name) => write!(f, "Function not found: {}", name),
            StateError::NotFoundModule(name) => write!(f, "Module not found: {}", name),
            StateError::NotFoundMemory(addr) => write!(f, "Memory not found: {}", addr),
            StateError::NotFoundTypes => write!(f, "Types not found"),
        }
    }
}

// ---------------------------------------------------------------------------
// StateGlobal
// ---------------------------------------------------------------------------

pub struct StateGlobal {
    data: Vec<Global>,
}

impl From<&Module> for StateGlobal {
    fn from(value: &Module) -> Self {
        Self {
            data: value.globals.iter().map(|g| g.clone()).collect(),
        }
    }
}

impl StateGlobal {
    pub fn set(&mut self, idx: GlobalIndex, value: Value) -> Result<()> {
        self.data[idx as usize] = Global {
            mutable: false,
            value,
        };
        Ok(())
    }

    pub fn get(&mut self, idx: GlobalIndex) -> Result<Value> {
        Ok(self.data[idx as usize].value.clone())
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

pub struct State {
    pub(crate) exports: Box<[Export]>,
    pub(crate) functions: Box<[Arc<Function>]>,
    pub(crate) function_types: Box<[FunctionType]>,
    pub(crate) global: StateGlobal,
    pub(crate) memories: Box<[Memory]>,
    pub(crate) tables: Box<[Table]>,
}

impl State {
    pub fn new(module: &Module) -> std::result::Result<Self, StateError> {
        Ok(Self {
            exports: module.exports.to_vec().into_boxed_slice(),
            functions: module.functions.clone(),
            function_types: module.function_types.clone(),
            global: StateGlobal::from(module),
            memories: module.memories.to_vec().into_boxed_slice(),
            tables: module.tables.to_vec().into_boxed_slice(),
        })
    }

    pub fn function(&self, idx: FunctionIndex) -> std::result::Result<Arc<Function>, Trap> {
        self.functions
            .get(idx as usize)
            .ok_or(Trap::NotFound(TrapNotFound::FunctionLocal(idx)))
            .map(|arc| arc.clone())
    }

    pub fn function_type(
        &self,
        idx: FunctionTypeIndex,
    ) -> std::result::Result<FunctionType, Trap> {
        self.function_types
            .get(idx as usize)
            .ok_or(Trap::NotFound(TrapNotFound::FunctionType(idx)))
            .map(|ft| ft.clone())
    }

    pub fn export(&self, name: impl Into<String>) -> std::result::Result<Export, Trap> {
        let name = name.into();
        self.exports
            .iter()
            .find(|export| export.name().eq(&name))
            .map(|e| e.clone())
            .ok_or(Trap::NotFound(TrapNotFound::ExportedFunction(name)))
    }

    pub fn memory(&self, idx: MemoryIndex) -> std::result::Result<&Memory, Trap> {
        self.memories
            .get(idx as usize)
            .ok_or(Trap::NotFound(TrapNotFound::Memory(idx)))
    }

    pub fn memory_mut(&mut self, idx: MemoryIndex) -> std::result::Result<&mut Memory, Trap> {
        self.memories
            .get_mut(idx as usize)
            .ok_or(Trap::NotFound(TrapNotFound::Memory(idx)))
    }

    pub fn table(&self, idx: TableIndex) -> std::result::Result<&Table, Trap> {
        self.tables
            .get(idx as usize)
            .ok_or(Trap::NotFound(TrapNotFound::Table(idx)))
    }

    pub fn table_at(
        &self,
        table_idx: TableIndex,
        element_idx: TableElementIndex,
    ) -> std::result::Result<Value, Trap> {
        let table = self.table(table_idx)?;

        let result = table
            .elements
            .get(element_idx as usize)
            .map(|e| e.clone())
            .ok_or(Trap::UndefinedElement)?
            .take()
            .ok_or(Trap::UninitializedElement)?;

        Ok(result)
    }
}
