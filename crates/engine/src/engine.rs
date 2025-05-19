// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, execute_plan, execute_plan_mut};
use auth::Principal;
use rql::ast;
use rql::ast::Ast;
use rql::plan::{plan, plan_mut};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use storage::StorageEngine;
use transaction::{Rx, TransactionEngine, Tx};

pub struct Engine<S: StorageEngine, T: TransactionEngine<S>>(Arc<EngineInner<S, T>>);

impl<S, T> Clone for Engine<S, T>
where
    S: StorageEngine,
    T: TransactionEngine<S>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: StorageEngine, T: TransactionEngine<S>> Deref for Engine<S, T> {
    type Target = EngineInner<S, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<S: StorageEngine, T: TransactionEngine<S>> {
    transaction: T,
    _marker: PhantomData<S>,
}

impl<S: StorageEngine, T: TransactionEngine<S>> Engine<S, T> {
    pub fn new(transaction: T) -> Self {
        Self(Arc::new(EngineInner { transaction, _marker: PhantomData }))
    }
}

impl<S: StorageEngine, T: TransactionEngine<S>> Engine<S, T> {
    fn begin(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin().unwrap())
    }

    pub fn begin_read_only(&self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_read_only().unwrap())
    }

    pub fn tx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let mut tx = self.begin().unwrap();

        for statement in statements {
            match &statement.0[0] {
                Ast::From(_) | Ast::Select(_) => {
                    let plan = plan(statement).unwrap();
                    let er = execute_plan(plan, &mut tx).unwrap();
                    result.push(er);
                }
                _ => {
                    let plan = plan_mut(tx.catalog().unwrap(), statement).unwrap();
                    let er = execute_plan_mut(plan, &mut tx).unwrap();
                    result.push(er);
                }
            }
        }

        tx.commit().unwrap();

        Ok(result)
    }

    pub fn rx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let rx = self.begin_read_only().unwrap();
        for statement in statements {
            let plan = plan(statement).unwrap();
            let er = execute_plan(plan, &rx).unwrap();
            result.push(er);
        }

        Ok(result)
    }
}
