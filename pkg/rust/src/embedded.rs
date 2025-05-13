// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::DB;
use engine::execute::{ExecutionResult, execute_plan, execute_plan_mut};
use engine::{Engine, Transaction, TransactionMut};
use rql::ast;
use rql::plan::{plan, plan_mut};
use storage::Memory;

pub struct Embedded {
    engine: engine::svl::Engine<Memory>,
}

impl Embedded {
    pub fn new() -> Self {
        Self { engine: engine::svl::Engine::new(Memory::default()) }
    }
}

impl DB for Embedded {
    fn tx_execute(&self, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let mut tx = self.engine.begin().unwrap();

        for statement in statements {
            let plan = plan_mut(tx.catalog().unwrap(), statement).unwrap();
            let er = execute_plan_mut(plan, &mut tx).unwrap();
            result.push(er);
        }

        tx.commit().unwrap();

        result
    }

    fn rx_execute(&self, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let rx = self.engine.begin_read_only().unwrap();
        for statement in statements {
            let plan = plan(statement).unwrap();
            let er = execute_plan(plan, &rx).unwrap();
            result.push(er);
        }

        result
    }
}
