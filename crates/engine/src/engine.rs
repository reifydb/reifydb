// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::{execute, execute_mut};
use auth::Principal;
use persistence::Persistence;
use rql::ast;
use rql::ast::Ast;
use rql::plan::{Plan, plan, plan_mut};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use transaction::{Rx, Transaction, Tx};

pub struct Engine<P: Persistence, T: Transaction<P>>(Arc<EngineInner<P, T>>);

impl<P, T> Clone for Engine<P, T>
where
    P: Persistence,
    T: Transaction<P>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P: Persistence, T: Transaction<P>> Deref for Engine<P, T> {
    type Target = EngineInner<P, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<P: Persistence, T: Transaction<P>> {
    transaction: T,
    _marker: PhantomData<P>,
}

impl<P: Persistence, T: Transaction<P>> Engine<P, T> {
    pub fn new(transaction: T) -> Self {
        Self(Arc::new(EngineInner { transaction, _marker: PhantomData }))
    }
}

impl<P: Persistence, T: Transaction<P>> Engine<P, T> {
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
                    let er = execute_mut(plan, &mut tx).unwrap();
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
            match plan {
                Plan::Query(plan) => {
                    let er = execute(plan, &rx).unwrap();
                    result.push(er);
                }
                _ => unimplemented!(),
            }
        }

        Ok(result)
    }
}
