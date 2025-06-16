// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{execute_rx, execute_tx};
use crate::{ExecutionResult, view};
use reifydb_auth::Principal;
use reifydb_core::hook::Hooks;
use reifydb_rql::ast;
use reifydb_rql::ast::Ast;
use reifydb_rql::plan::{plan_rx, plan_tx};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::{Transaction, Tx};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>>(
    Arc<EngineInner<VS, US, T>>,
);

impl<VS, US, T> Clone for Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Deref
    for Engine<VS, US, T>
{
    type Target = EngineInner<VS, US, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> {
    transaction: T,
    hooks: Hooks,
    deferred_view: Arc<view::deferred::Engine<VS, US>>,
    _marker: PhantomData<(VS, US)>,
}

impl<VS: VersionedStorage + 'static, US: UnversionedStorage + 'static, T: Transaction<VS, US>>
    Engine<VS, US, T>
{
    pub fn new(transaction: T) -> Self {
        let storage = transaction.versioned();
        let deferred_view = view::deferred::Engine::new(storage);
        let hooks = transaction.hooks();
        let result =
            Self(Arc::new(EngineInner { transaction, hooks, deferred_view, _marker: PhantomData }));
        result.setup_hooks();
        result
    }
}

impl<VS: VersionedStorage + 'static, US: UnversionedStorage + 'static, T: Transaction<VS, US>>
    Engine<VS, US, T>
{
    pub fn setup_hooks(&self) {
        self.hooks.transaction().post_commit().register(self.deferred_view.clone());
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Engine<VS, US, T> {
    pub fn begin(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin().unwrap())
    }

    pub fn begin_read_only(&self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_read_only().unwrap())
    }

    pub fn tx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<ExecutionResult>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut tx = self.begin().unwrap();

        // let mut storage = self.transaction.versioned();

        for statement in statements {
            match &statement.0[0] {
                Ast::From(_) | Ast::Select(_) => {
                    let plan = plan_rx(statement)?;
                    let er = execute_rx::<VS, US>(&mut tx, plan)?;
                    result.push(er);
                }
                _ => {
                    let plan = plan_tx::<VS, US>(&mut tx, statement)?;
                    let er = execute_tx(&mut tx, plan)?;
                    result.push(er);
                }
            }
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
            let er = execute_rx::<VS, US>(&mut rx, plan).unwrap();
            result.push(er);
        }

        Ok(result)
    }
}
