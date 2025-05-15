// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::DB;
use engine::Engine;
use engine::execute::{ExecutionResult, execute_plan, execute_plan_mut};
use rql::ast;
use rql::plan::{plan, plan_mut};
use storage::Memory;
use transaction::{Transaction, TransactionMut};

pub struct Embedded {
    engine: Engine<'static, Memory, transaction::mvcc::Engine<Memory>>,
}

impl Embedded {
    pub fn new() -> Self {
        Self { engine: Engine::new(transaction::mvcc::Engine::new(Memory::default())) }
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
