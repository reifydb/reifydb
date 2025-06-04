// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::{execute, execute_mut};
use reifydb_auth::Principal;
use reifydb_rql::ast;
use reifydb_rql::ast::Ast;
use reifydb_rql::plan::{Plan, plan, plan_mut};
use reifydb_storage::Storage;
use reifydb_transaction::{Rx, Transaction, Tx};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<S: Storage, T: Transaction<S>>(Arc<EngineInner<S, T>>);

impl<S, T> Clone for Engine<S, T>
where
    S: Storage,
    T: Transaction<S>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: Storage, T: Transaction<S>> Deref for Engine<S, T> {
    type Target = EngineInner<S, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<S: Storage, T: Transaction<S>> {
    transaction: T,
    _marker: PhantomData<S>,
}

impl<S: Storage, T: Transaction<S>> Engine<S, T> {
    pub fn new(transaction: T) -> Self {
        Self(Arc::new(EngineInner { transaction, _marker: PhantomData }))
    }
}

impl<S: Storage, T: Transaction<S>> Engine<S, T> {
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
                    let er = execute_mut(plan, &mut tx).unwrap();
                    result.push(er);
                }
                _ => {
                    let plan = plan_mut(tx.catalog().unwrap(), statement).unwrap();
                    let er = execute_mut(plan, &mut tx)?;
                    result.push(er);
                }
            }
        }

        tx.commit().unwrap();
        // tx.rollback().unwrap();

        Ok(result)
    }

    pub fn rx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let mut rx = self.begin_read_only().unwrap();
        for statement in statements {
            let plan = plan(statement).unwrap();
            match plan {
                Plan::Query(plan) => {
                    let er = execute(plan, &mut rx).unwrap();
                    result.push(er);
                }
                _ => unimplemented!(),
            }
        }

        Ok(result)
    }
}
