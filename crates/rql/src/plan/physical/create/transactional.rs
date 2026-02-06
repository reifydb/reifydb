// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateTransactionalView;
use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	plan::{
		logical,
		physical::{Compiler, CreateTransactionalViewNode, PhysicalPlan},
	},
	query::QueryPlan,
};

impl Compiler {
	pub(crate) fn compile_create_transactional<T: AsTransaction>(
		&self,
		rx: &mut T,
		create: logical::CreateTransactionalViewNode<'_>,
	) -> crate::Result<PhysicalPlan> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.view.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = create
				.view
				.namespace
				.map(|n| n.to_owned())
				.unwrap_or_else(|| Fragment::internal("default".to_string()));
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		let physical_plan = self.compile(rx, create.as_clause)?.unwrap();
		let query_plan: QueryPlan = physical_plan.try_into().expect("AS clause must be a query plan");

		Ok(CreateTransactionalView(CreateTransactionalViewNode {
			namespace,
			view: create.view.name.to_owned(),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			as_clause: Box::new(query_plan),
			primary_key: super::materialize_primary_key(create.primary_key),
		}))
	}
}
