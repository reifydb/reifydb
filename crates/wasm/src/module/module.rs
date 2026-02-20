// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::module::{Export, Function, FunctionType, Global, Memory, Table};

pub type ModuleId = u16;

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

pub struct Module {
    pub id: ModuleId,
    pub exports: Box<[Export]>,
    pub functions: Box<[Arc<Function>]>,
    pub function_types: Box<[FunctionType]>,
    pub globals: Box<[Global]>,
    pub memories: Box<[Memory]>,
    pub tables: Box<[Table]>,
}

impl Module {
    pub fn new(
        id: ModuleId,
        exports: Box<[Export]>,
        functions: Box<[Arc<Function>]>,
        function_types: Box<[FunctionType]>,
        globals: Box<[Global]>,
        memories: Box<[Memory]>,
        tables: Box<[Table]>,
    ) -> Self {
        Self {
            id,
            exports,
            functions,
            function_types,
            globals,
            memories,
            tables,
        }
    }
}
