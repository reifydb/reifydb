// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{execute_rx, execute_tx};
use crate::{ExecutionResult, view};
use reifydb_auth::Principal;
use reifydb_core::hook::Hooks;
use reifydb_rql::ast;
use reifydb_rql::plan::{plan_rx, plan_tx};
use reifydb_storage::Storage;
use reifydb_transaction::{Transaction, Tx};
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
    hooks: Hooks,
    deferred_view: Arc<view::deferred::Engine<S>>,
    _marker: PhantomData<S>,
}

impl<S: Storage + 'static, T: Transaction<S>> Engine<S, T> {
    pub fn new(transaction: T) -> Self {
        let storage = transaction.storage();
        let deferred_view = view::deferred::Engine::new(storage);
        let hooks = transaction.hooks();
        let result =
            Self(Arc::new(EngineInner { transaction, hooks, deferred_view, _marker: PhantomData }));
        result.setup_hooks();
        result
    }
}

impl<S: Storage + 'static, T: Transaction<S>> Engine<S, T> {
    pub fn setup_hooks(&self) {
        self.hooks.transaction().post_commit().register(self.deferred_view.clone());
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
        let statements = ast::parse(rql)?;

        let mut tx = self.begin().unwrap();

        for statement in statements {
            // match &statement.0[0] {
            // Ast::From(_) | Ast::Select(_) => {
            //     let plan = plan_rx(statement)?;
            //     let er = execute_tx(plan, &mut tx)?;
            //     result.push(er);
            // }
            // _ => {
            let plan = plan_tx(&tx, statement)?;
            let er = execute_tx(plan, &mut tx)?;
            result.push(er);
            // }
            // }
        }

        tx.commit().unwrap();

        Ok(result)
    }

    pub fn rx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut rx = self.begin_read_only().unwrap();
        for statement in statements {
            let plan = plan_rx(statement).unwrap();
            let er = execute_rx(plan, &mut rx).unwrap();
            result.push(er);
        }

        Ok(result)
    }
}
