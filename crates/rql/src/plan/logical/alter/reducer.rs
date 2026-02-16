// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::reducer::ReducerColumnToCreate;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::{
		ast::{AstAlterReducer, AstAlterReducerAction, AstType},
		identifier::MaybeQualifiedReducerIdentifier,
	},
	bump::{BumpFragment, BumpVec},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug)]
pub struct AlterReducerNode<'bump> {
	pub reducer: MaybeQualifiedReducerIdentifier<'bump>,
	pub action: LogicalAlterReducerAction<'bump>,
}

#[derive(Debug)]
pub enum LogicalAlterReducerAction<'bump> {
	AddAction {
		name: BumpFragment<'bump>,
		columns: Vec<ReducerColumnToCreate>,
		on_dispatch: BumpVec<'bump, LogicalPlan<'bump>>,
	},
	AlterAction {
		name: BumpFragment<'bump>,
		on_dispatch: BumpVec<'bump, LogicalPlan<'bump>>,
	},
	DropAction {
		name: BumpFragment<'bump>,
	},
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_reducer<T: AsTransaction>(
		&self,
		ast: AstAlterReducer<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		let reducer = ast.reducer.clone();

		let action = match ast.action {
			AstAlterReducerAction::AddAction {
				action_name,
				columns,
				on_dispatch,
			} => {
				let mut reducer_columns = Vec::new();
				for col in columns.into_iter() {
					let constraint = convert_data_type_with_constraints(&col.ty)?;

					let name = col.name.to_owned();
					let ty_fragment = match &col.ty {
						AstType::Unconstrained(f) => f.to_owned(),
						AstType::Constrained {
							name,
							..
						} => name.to_owned(),
					};
					let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

					reducer_columns.push(ReducerColumnToCreate {
						name,
						fragment,
						constraint,
					});
				}

				let compiled_on_dispatch = self.compile(on_dispatch, tx)?;
				LogicalAlterReducerAction::AddAction {
					name: action_name,
					columns: reducer_columns,
					on_dispatch: compiled_on_dispatch,
				}
			}
			AstAlterReducerAction::AlterAction {
				action_name,
				on_dispatch,
			} => {
				let compiled_on_dispatch = self.compile(on_dispatch, tx)?;
				LogicalAlterReducerAction::AlterAction {
					name: action_name,
					on_dispatch: compiled_on_dispatch,
				}
			}
			AstAlterReducerAction::DropAction {
				action_name,
			} => LogicalAlterReducerAction::DropAction {
				name: action_name,
			},
		};

		Ok(LogicalPlan::AlterReducer(AlterReducerNode {
			reducer,
			action,
		}))
	}
}
