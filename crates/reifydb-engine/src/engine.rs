// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, ops::Deref, sync::Arc};

use reifydb_core::{
	Frame, Type,
	hook::{BoxedHookIter, Callback, Hooks, transaction::PostCommitHook},
	interface::{
		ActiveCommandTransaction, ActiveQueryTransaction, Command,
		Engine as EngineInterface, ExecuteCommand, ExecuteQuery,
		GetHooks, Key, Params, Identity, Query, TableId, Transaction,
		VersionedTransaction,
	},
	return_hooks,
	row::EncodedRowLayout,
};

use crate::{
	columnar::{Column, ColumnData, ColumnQualified, Columns},
	execute::Executor,
	flow::{
		change::{Change, Diff},
		flow::Flow,
		node::NodeType,
		processor::FlowProcessor,
	},
	function::{Functions, math},
	subsystem::init::register_system_hooks,
};

pub struct Engine<T: Transaction>(Arc<EngineInner<T>>);

impl<T: Transaction> GetHooks for Engine<T> {
	fn get_hooks(&self) -> &Hooks {
		&self.hooks
	}
}

impl<T: Transaction> EngineInterface<T> for Engine<T> {
	fn begin_command(&self) -> crate::Result<ActiveCommandTransaction<T>> {
		Ok(ActiveCommandTransaction::new(
			self.versioned.begin_command()?,
			self.unversioned.clone(),
			self.cdc.clone(),
		))
	}

	fn begin_query(&self) -> crate::Result<ActiveQueryTransaction<T>> {
		Ok(ActiveQueryTransaction::new(
			self.versioned.begin_query()?,
			self.unversioned.clone(),
			self.cdc.clone(),
		))
	}

	fn command_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: Params,
	) -> crate::Result<Vec<Frame>> {
		let mut txn = self.begin_command()?;
		let result = self.execute_command(
			&mut txn,
			Command {
				rql,
				params,
				identity,
			},
		)?;
		txn.commit()?;
		Ok(result)
	}

	fn query_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: Params,
	) -> crate::Result<Vec<Frame>> {
		let mut txn = self.begin_query()?;
		let result = self.execute_query(
			&mut txn,
			Query {
				rql,
				params,
				identity,
			},
		)?;
		Ok(result)
	}
}

impl<T: Transaction> ExecuteCommand<T> for Engine<T> {
	#[inline]
	fn execute_command<'a>(
		&'a self,
		txn: &mut ActiveCommandTransaction<T>,
		cmd: Command<'a>,
	) -> crate::Result<Vec<Frame>> {
		self.executor.execute_command(txn, cmd)
	}
}

impl<T: Transaction> ExecuteQuery<T> for Engine<T> {
	#[inline]
	fn execute_query<'a>(
		&'a self,
		txn: &mut ActiveQueryTransaction<T>,
		qry: Query<'a>,
	) -> crate::Result<Vec<Frame>> {
		self.executor.execute_query(txn, qry)
	}
}

impl<T: Transaction> Clone for Engine<T> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<T: Transaction> Deref for Engine<T> {
	type Target = EngineInner<T>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub struct EngineInner<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	executor: Executor<T>,

	_processor: FlowProcessor<T>, // FIXME remove me
}

impl<T: Transaction> Engine<T> {
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> crate::Result<Self> {
		let result = Self(Arc::new(EngineInner {
			versioned: versioned.clone(),
			unversioned: unversioned.clone(),
			cdc: cdc.clone(),
			hooks,
			executor: Executor {
				functions: Functions::builder()
					.register_aggregate(
						"sum",
						math::aggregate::Sum::new,
					)
					.register_aggregate(
						"min",
						math::aggregate::Min::new,
					)
					.register_aggregate(
						"max",
						math::aggregate::Max::new,
					)
					.register_aggregate(
						"avg",
						math::aggregate::Avg::new,
					)
					.register_scalar(
						"abs",
						math::scalar::Abs::new,
					)
					.register_scalar(
						"avg",
						math::scalar::Avg::new,
					)
					.build(),
				_phantom: PhantomData,
			},
			_processor: FlowProcessor::new(
				Flow::default(),
				versioned,
				unversioned,
				cdc,
			),
		}));

		result.setup_hooks()?;
		Ok(result)
	}

	pub fn versioned(&self) -> &T::Versioned {
		&self.versioned
	}

	pub fn unversioned(&self) -> &T::Unversioned {
		&self.unversioned
	}
}

#[allow(dead_code)]
struct FlowPostCommit<T: Transaction> {
	engine: Engine<T>,
}

impl<T: Transaction> Callback<PostCommitHook> for FlowPostCommit<T> {
	fn on(&self, hook: &PostCommitHook) -> crate::Result<BoxedHookIter> {
		println!("Transaction version: {}", hook.version);

		for delta in hook.deltas.iter() {
			match Key::decode(delta.key()).unwrap() {
				Key::TableRow(key) => {
					if key.table == TableId(3) {
						continue;
					}

					// dbg!(key.table);
					let layout = EncodedRowLayout::new(&[
						Type::Utf8,
						Type::Int1,
					]);
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

					let mut txn = self
						.engine
						.begin_command()
						.unwrap();

					let frame = self
						.engine
						.query_as(
							&Identity::root(),
							"FROM reifydb.flows filter { id == 1 } map { cast(data, utf8) }",
							Params::None,
						)
						.unwrap()
						.pop()
						.unwrap();

					let value = frame[0].get_value(0);
					// dbg!(&value.to_string());

					let flow: Flow = serde_json::from_str(
						value.to_string().as_str(),
					)
					.unwrap();
					// dbg!(&flow);

					// Find the source node (users table)
					let source_node_id =
						flow.get_all_nodes()
							.find(|node_id| {
								if let Some(node) = flow.get_node(node_id) {
                                matches!(node.ty, NodeType::Source { .. })
                            } else {
                                false
                            }
							})
							.expect(
								"Should have a source node",
							);

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

impl<T: Transaction> Engine<T> {
	pub fn setup_hooks(&self) -> crate::Result<()> {
		register_system_hooks(&self);

		// self.hooks.register(FlowPostCommit { engine: self.clone() });

		Ok(())
	}
}
