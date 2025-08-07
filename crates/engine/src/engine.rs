// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{Column, ColumnData, ColumnQualified, Columns};
use crate::execute::Executor;
use crate::flow::change::{Change, Diff};
use crate::flow::flow::Flow;
use crate::flow::node::NodeType;
use crate::flow::processor::FlowProcessor;
use crate::function::{Functions, math};
use crate::subsystem::init::register_system_hooks;
use reifydb_core::hook::transaction::PostCommitHook;
use reifydb_core::hook::{BoxedHookIter, Callback, Hooks};
use reifydb_core::interface::{
    ActiveCommandTransaction, ActiveQueryTransaction, Command, Engine as EngineInterface,
    ExecuteCommand, ExecuteQuery, GetHooks, Key, Params, Principal, Query, TableId,
    UnversionedTransaction, VersionedCommandTransaction, VersionedTransaction,
};
use reifydb_core::row::EncodedRowLayout;
use reifydb_core::{Frame, Type, return_hooks};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<VT, UT>(Arc<EngineInner<VT, UT>>)
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction;

impl<VT, UT> GetHooks for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn get_hooks(&self) -> &Hooks {
        &self.hooks
    }
}

impl<VT, UT> EngineInterface<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn begin_command(&self) -> crate::Result<ActiveCommandTransaction<VT, UT>> {
        Ok(ActiveCommandTransaction::new(self.versioned.begin_command()?, self.unversioned.clone()))
    }

    fn begin_query(&self) -> crate::Result<ActiveQueryTransaction<VT, UT>> {
        Ok(ActiveQueryTransaction::new(self.versioned.begin_query()?, self.unversioned.clone()))
    }

    fn command_as(
        &self,
        principal: &Principal,
        rql: &str,
        params: Params,
    ) -> crate::Result<Vec<Frame>> {
        let mut txn = self.begin_command()?;
        let result = self.execute_command(&mut txn, Command { rql, params, principal })?;
        txn.commit()?;
        Ok(result)
    }

    fn query_as(
        &self,
        principal: &Principal,
        rql: &str,
        params: Params,
    ) -> crate::Result<Vec<Frame>> {
        let mut txn = self.begin_query()?;
        let result = self.execute_query(&mut txn, Query { rql, params, principal })?;
        Ok(result)
    }
}

impl<VT, UT> ExecuteCommand<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn execute_command<'a>(
        &'a self,
        txn: &mut ActiveCommandTransaction<VT, UT>,
        cmd: Command<'a>,
    ) -> crate::Result<Vec<Frame>> {
        self.executor.execute_command(txn, cmd)
    }
}

impl<VT, UT> ExecuteQuery<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn execute_query<'a>(
        &'a self,
        txn: &mut ActiveQueryTransaction<VT, UT>,
        qry: Query<'a>,
    ) -> crate::Result<Vec<Frame>> {
        self.executor.execute_query(txn, qry)
    }
}

impl<VT, UT> Clone for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VT, UT> Deref for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    type Target = EngineInner<VT, UT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    versioned: VT,
    unversioned: UT,
    hooks: Hooks,
    executor: Executor<VT, UT>,

    _processor: FlowProcessor<VT, UT>, // FIXME remove me
}

impl<VT, UT> Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        let result = Self(Arc::new(EngineInner {
            versioned: versioned.clone(),
            unversioned: unversioned.clone(),
            hooks,
            executor: Executor {
                functions: Functions::builder()
                    .register_aggregate("sum", math::aggregate::Sum::new)
                    .register_aggregate("min", math::aggregate::Min::new)
                    .register_aggregate("max", math::aggregate::Max::new)
                    .register_aggregate("avg", math::aggregate::Avg::new)
                    .register_scalar("abs", math::scalar::Abs::new)
                    .register_scalar("avg", math::scalar::Avg::new)
                    .build(),
                _phantom: PhantomData,
            },
            _processor: FlowProcessor::new(Flow::default(), versioned, unversioned),
        }));

        result.setup_hooks()?;
        Ok(result)
    }

    pub fn versioned(&self) -> &VT {
        &self.versioned
    }

    pub fn unversioned(&self) -> &UT {
        &self.unversioned
    }
}

#[allow(dead_code)]
struct FlowPostCommit<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
}

impl<VT, UT> Callback<PostCommitHook> for FlowPostCommit<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn on(&self, hook: &PostCommitHook) -> crate::Result<BoxedHookIter> {
        println!("Transaction version: {}", hook.version);

        for delta in hook.deltas.iter() {
            match Key::decode(delta.key()).unwrap() {
                Key::TableRow(key) => {
                    if key.table == TableId(3) {
                        continue;
                    }

                    // dbg!(key.table);
                    let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Int1]);
                    let row = delta.row().unwrap();

                    let name = layout.get_utf8(&row, 0);
                    let age = layout.get_i8(&row, 1);

                    println!("{name}: {age}");

                    let columns = Columns::new(vec![
                        Column::ColumnQualified(ColumnQualified {
                            name: "name".to_string(),
                            data: ColumnData::utf8([name.to_string()]),
                        }),
                        Column::ColumnQualified(ColumnQualified {
                            name: "age".to_string(),
                            data: ColumnData::int1([age]),
                        }),
                    ]);

                    let mut txn = self.engine.begin_command().unwrap();

                    let frame = self
                        .engine
                        .query_as(
                            &Principal::root(),
                            "FROM reifydb.flows filter { id == 1 } map { cast(data, utf8) }",
                            Params::None,
                        )
                        .unwrap()
                        .pop()
                        .unwrap();

                    let value = frame[0].get_value(0);
                    // dbg!(&value.to_string());

                    let flow: Flow = serde_json::from_str(value.to_string().as_str()).unwrap();
                    // dbg!(&flow);

                    // Find the source node (users table)
                    let source_node_id = flow
                        .get_all_nodes()
                        .find(|node_id| {
                            if let Some(node) = flow.get_node(node_id) {
                                matches!(node.ty, NodeType::Source { .. })
                            } else {
                                false
                            }
                        })
                        .expect("Should have a source node");

                    self.engine
                        ._processor
                        .hack(
                            &flow,
                            &mut txn,
                            &source_node_id,
                            Change {
                                diffs: vec![Diff::Insert { columns }],
                                metadata: Default::default(),
                            },
                        )
                        .unwrap();

                    txn.commit().unwrap();

                    // dbg!(&columns);
                }
                _ => {}
            };
        }
        return_hooks!()
    }
}

impl<VT, UT> Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn setup_hooks(&self) -> crate::Result<()> {
        register_system_hooks(&self);

        // self.hooks.register(FlowPostCommit { engine: self.clone() });

        Ok(())
    }
}
