// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::{NamespaceId, QueueId, ViewId};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, fragment::Fragment};

use crate::{
	CatalogStore,
	catalog::Catalog,
	error::{CatalogError, NameKind},
};

#[derive(Debug, Clone, Copy, Default)]
pub struct NameAllowed {
	pub queue: Option<QueueId>,
	pub view: Option<ViewId>,
	pub namespace: Option<NamespaceId>,
}

impl Catalog {
	pub fn reject_name_taken(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &Fragment,
		creating: NameKind,
		allowed: NameAllowed,
	) -> Result<(), ValueError> {
		let text = name.text();
		let Some(existing) = self.find_name_holder(txn, namespace, text, creating, allowed)? else {
			return Ok(());
		};
		let parent = self.get_namespace(txn, namespace)?;
		Err(CatalogError::NameTaken {
			existing,
			namespace: parent.name().to_string(),
			name: text.to_string(),
			fragment: name.clone(),
		}
		.into())
	}

	fn find_name_holder(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		text: &str,
		creating: NameKind,
		allowed: NameAllowed,
	) -> Result<Option<NameKind>, ValueError> {
		if creating != NameKind::Table && self.find_table_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Table));
		}
		if creating != NameKind::RingBuffer && self.find_ringbuffer_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::RingBuffer));
		}
		if creating != NameKind::View
			&& let Some(view) = self.find_view_by_name(txn, namespace, text)?
			&& allowed.view != Some(view.id())
		{
			return Ok(Some(NameKind::View));
		}
		if creating != NameKind::Dictionary && self.find_dictionary_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Dictionary));
		}
		if creating != NameKind::Series && self.find_series_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Series));
		}
		if creating != NameKind::Queue
			&& let Some(queue) = self.find_queue_by_name(txn, namespace, text)?
			&& allowed.queue != Some(queue.id)
		{
			return Ok(Some(NameKind::Queue));
		}
		if creating != NameKind::EnumOrEvent && self.find_sumtype_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::EnumOrEvent));
		}
		if creating != NameKind::Procedure
			&& creating != NameKind::Handler
			&& self.find_procedure_by_name(txn, namespace, text)?.is_some()
		{
			return Ok(Some(NameKind::Procedure));
		}
		if creating != NameKind::Handler
			&& creating != NameKind::Procedure
			&& self.find_handler_by_name(txn, namespace, text)?.is_some()
		{
			return Ok(Some(NameKind::Handler));
		}
		if creating != NameKind::Flow && self.find_flow_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Flow));
		}
		if creating != NameKind::WindowedViewGroup
			&& self.find_windowed_view_group_by_name(txn, namespace, text)?.is_some()
		{
			return Ok(Some(NameKind::WindowedViewGroup));
		}
		if creating != NameKind::Sink && self.find_sink_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Sink));
		}
		if creating != NameKind::Source && self.find_source_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Source));
		}
		if self.find_test_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Test));
		}
		if creating != NameKind::Binding && self.find_binding_by_name(txn, namespace, text)?.is_some() {
			return Ok(Some(NameKind::Binding));
		}
		if creating != NameKind::Namespace {
			let parent = self.get_namespace(txn, namespace)?;
			if let Some(child) =
				self.find_namespace_by_name(txn, &format!("{}::{}", parent.name(), text))?
				&& allowed.namespace != Some(child.id())
			{
				return Ok(Some(NameKind::Namespace));
			}
		}
		if creating != NameKind::VirtualTable && self.find_vtable_user_by_name(txn, namespace, text).is_some() {
			return Ok(Some(NameKind::VirtualTable));
		}
		Ok(None)
	}

	pub fn find_namespace_leftover(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
	) -> Result<Option<(NameKind, String)>, ValueError> {
		if let Some(child) = self.list_namespaces_all(txn)?.into_iter().find(|ns| ns.parent_id() == namespace) {
			return Ok(Some((NameKind::Namespace, child.local_name().to_string())));
		}
		if let Some(series) = self.list_series(txn)?.into_iter().find(|s| s.namespace == namespace) {
			return Ok(Some((NameKind::Series, series.name)));
		}
		if let Some(procedure) =
			CatalogStore::list_procedures_all(txn)?.into_iter().find(|p| p.namespace() == namespace)
		{
			return Ok(Some((NameKind::Procedure, procedure.name().to_string())));
		}
		if let Some(handler) = CatalogStore::find_first_handler_name_in_namespace(txn, namespace)? {
			return Ok(Some((NameKind::Handler, handler)));
		}
		if let Some(sink) = self.list_sinks_all(txn)?.into_iter().find(|s| s.namespace == namespace) {
			return Ok(Some((NameKind::Sink, sink.name)));
		}
		if let Some(source) = self.list_sources_all(txn)?.into_iter().find(|s| s.namespace == namespace) {
			return Ok(Some((NameKind::Source, source.name)));
		}
		if let Some(test) = self.list_tests_in_namespace(txn, namespace)?.into_iter().next() {
			return Ok(Some((NameKind::Test, test.name)));
		}
		if let Some(binding) =
			CatalogStore::list_bindings_all(txn)?.into_iter().find(|b| b.namespace == namespace)
		{
			return Ok(Some((NameKind::Binding, binding.name)));
		}
		if let Some(vtable) = self.list_user_vtables().into_iter().find(|v| v.namespace == namespace) {
			return Ok(Some((NameKind::VirtualTable, vtable.name.clone())));
		}
		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::TimeSource,
		flow::dag::FlowDag,
		interface::catalog::{
			binding::{BindingFormat, BindingProtocol},
			id::NamespaceId,
			procedure::{RqlTrigger, RunAs},
			queue::{QueueDispatch, QueueRetention, QueueRetry},
			series::{SeriesKey, TimestampPrecision},
			view::View,
		},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::{
		error::ValueError,
		fragment::Fragment,
		value::{
			sumtype::{SumTypeId, VariantRef},
			value_type::ValueType,
		},
	};

	use super::NameAllowed;
	use crate::{
		catalog::{
			Catalog, binding::BindingToCreate, dictionary::DictionaryToCreate, flow::FlowToCreate,
			procedure::ProcedureToCreate, queue::QueueToCreate, series::SeriesToCreate,
			table::TableToCreate, test::TestToCreate, view::ViewToCreate,
		},
		store::view::create::ViewStorage,
		test_utils::{
			create_flow, create_namespace, create_ringbuffer, create_sink, create_source, create_sumtype,
			create_table, create_view,
		},
	};

	fn assert_taken_by(result: Result<impl std::fmt::Debug, ValueError>, kind: &str) {
		let err = result.expect_err("the create under a taken name must be refused");
		assert_eq!(err.code, "CA_137", "{}", err.message);
		assert!(err.message.ends_with(&format!("is already taken by a {kind}")), "{}", err.message);
	}

	fn bare_flow(
		catalog: &Catalog,
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		name: &str,
		allowed: NameAllowed,
	) -> Result<(), ValueError> {
		let id = catalog.next_flow_id(txn)?;
		catalog.create_flow(
			txn,
			FlowToCreate {
				name: Fragment::internal(name),
				namespace,
				allowed,
			},
			FlowDag::builder(id).build(),
		)?;
		Ok(())
	}

	fn rql_procedure(name: &str, namespace: NamespaceId) -> ProcedureToCreate {
		ProcedureToCreate::Rql {
			name: Fragment::internal(name),
			namespace,
			params: vec![],
			return_type: None,
			body: "\"A\"".to_string(),
			trigger: RqlTrigger::Call,
			run_as: RunAs::Caller,
		}
	}

	fn table_to_create(name: &str, namespace: NamespaceId) -> TableToCreate {
		TableToCreate {
			name: Fragment::internal(name),
			namespace,
			columns: vec![],
			primary_key_columns: None,
			partition_by: vec![],
			time: TimeSource::Processing,
		}
	}

	fn handler_variant() -> VariantRef {
		VariantRef {
			sumtype_id: SumTypeId(0),
			variant_tag: 0,
		}
	}

	fn make_first(kind: &str, catalog: &Catalog, txn: &mut AdminTransaction, namespace: NamespaceId) {
		match kind {
			"table" => {
				create_table(txn, "test", "x", &[]);
			}
			"ring buffer" => {
				create_ringbuffer(txn, "test", "x", 4, &[]);
			}
			"view" => {
				create_view(txn, "test", "x", &[]);
			}
			"enum or event" => {
				create_sumtype(txn, "test", "x", vec![]);
			}
			"handler" => {
				catalog.create_handler(
					txn,
					crate::catalog::handler::HandlerToCreate {
						name: Fragment::internal("x"),
						namespace,
						variant: handler_variant(),
						body_source: String::new(),
					},
				)
				.expect("the first object is created");
			}
			"sink" => {
				create_sink(txn, "test", "x", "postgres");
			}
			"source" => {
				create_source(txn, "test", "x", "postgres");
			}
			"namespace" => {
				create_namespace(txn, "test::x");
			}
			"dictionary" => {
				catalog.create_dictionary(
					txn,
					DictionaryToCreate {
						name: Fragment::internal("x"),
						namespace,
						value_type: ValueType::Utf8,
						id_type: ValueType::Uint4,
					},
				)
				.expect("the first object is created");
			}
			"series" => {
				catalog.create_series(
					txn,
					SeriesToCreate {
						name: Fragment::internal("x"),
						namespace,
						columns: vec![],
						tag: None,
						key: SeriesKey::DateTime {
							column: "ts".to_string(),
							precision: TimestampPrecision::Millisecond,
						},
						partition_by: vec![],
						time: TimeSource::Processing,
						encoding: None,
					},
				)
				.expect("the first object is created");
			}
			"queue" => {
				catalog.create_queue(
					txn,
					QueueToCreate {
						name: Fragment::internal("x"),
						namespace,
						columns: vec![],
						partition_by: vec![],
						dispatch: QueueDispatch::Fifo {
							ordered_by: None,
						},
						deduplicate: None,
						retention: QueueRetention::default(),
						retry: QueueRetry::default(),
						time: TimeSource::Processing,
					},
				)
				.expect("the first object is created");
			}
			"procedure" => {
				catalog.create_procedure(txn, rql_procedure("x", namespace))
					.expect("the first object is created");
			}
			"test" => {
				catalog.create_test(
					txn,
					TestToCreate {
						name: Fragment::internal("x"),
						namespace,
						cases: None,
						body: "CALL test::p()".to_string(),
					},
				)
				.expect("the first object is created");
			}
			"binding" => {
				let p = catalog
					.create_procedure(txn, rql_procedure("p", namespace))
					.expect("the procedure is created");
				catalog.create_binding(
					txn,
					BindingToCreate {
						namespace,
						name: "x".to_string(),
						procedure: p.id(),
						protocol: BindingProtocol::Grpc {
							name: "x_g".to_string(),
						},
						format: BindingFormat::Json,
					},
				)
				.expect("the first object is created");
			}
			other => panic!("no recipe for kind {other}"),
		}
	}

	#[test]
	fn a_bare_flow_under_a_taken_name_is_refused() {
		// Without the check in create_flow a flow made from Rust takes a name another kind already holds.
		for kind in [
			"table",
			"ring buffer",
			"view",
			"dictionary",
			"series",
			"queue",
			"enum or event",
			"procedure",
			"handler",
			"sink",
			"source",
			"test",
			"binding",
			"namespace",
		] {
			let mut txn = create_test_admin_transaction();
			let namespace = create_namespace(&mut txn, "test");
			let catalog = Catalog::testing();
			make_first(kind, &catalog, &mut txn, namespace.id());

			let result = bare_flow(&catalog, &mut txn, namespace.id(), "x", NameAllowed::default());

			assert_taken_by(result, kind);
			assert!(
				catalog.find_flow_by_name(&mut Transaction::Admin(&mut txn), namespace.id(), "x")
					.unwrap()
					.is_none(),
				"no flow x may exist after the refusal, first kind {kind}"
			);
		}
	}

	#[test]
	fn a_flow_passes_only_its_named_partner() {
		// A partner rule that matches by kind and not by id, or stops after the first partner, lets a stranger
		// in.
		let mut txn = create_test_admin_transaction();
		let namespace = create_namespace(&mut txn, "test");
		let catalog = Catalog::testing();
		let v = create_view(&mut txn, "test", "v", &[]);
		let w = create_view(&mut txn, "test", "w", &[]);
		let wrong_view = NameAllowed {
			view: Some(w.id()),
			..NameAllowed::default()
		};
		assert_taken_by(bare_flow(&catalog, &mut txn, namespace.id(), "v", wrong_view), "view");
		let right_view = NameAllowed {
			view: Some(v.id()),
			..NameAllowed::default()
		};
		bare_flow(&catalog, &mut txn, namespace.id(), "v", right_view).expect("the flow of view v is let in");

		let mut txn = create_test_admin_transaction();
		let namespace = create_namespace(&mut txn, "test");
		let child = create_namespace(&mut txn, "test::c");
		let other = create_namespace(&mut txn, "other");
		let wrong_namespace = NameAllowed {
			namespace: Some(other.id()),
			..NameAllowed::default()
		};
		assert_taken_by(bare_flow(&catalog, &mut txn, namespace.id(), "c", wrong_namespace), "namespace");
		let right_namespace = NameAllowed {
			namespace: Some(child.id()),
			..NameAllowed::default()
		};
		bare_flow(&catalog, &mut txn, namespace.id(), "c", right_namespace)
			.expect("the flow of namespace c is let in");

		let mut txn = create_test_admin_transaction();
		let namespace = create_namespace(&mut txn, "test");
		let queue = catalog
			.create_queue(
				&mut txn,
				QueueToCreate {
					name: Fragment::internal("q"),
					namespace: namespace.id(),
					columns: vec![],
					partition_by: vec![],
					dispatch: QueueDispatch::Fifo {
						ordered_by: None,
					},
					deduplicate: None,
					retention: QueueRetention::default(),
					retry: QueueRetry::default(),
					time: TimeSource::Processing,
				},
			)
			.expect("the queue is created");
		let view: View = catalog
			.create_deferred_view(
				&mut txn,
				ViewToCreate {
					name: Fragment::internal("q"),
					namespace: namespace.id(),
					columns: vec![],
					storage: ViewStorage::Queue {
						queue: queue.id,
						partition_by: vec![],
					},
					sort: vec![],
				},
			)
			.expect("the queue view is created over its own queue");
		let view_only = NameAllowed {
			view: Some(view.id()),
			..NameAllowed::default()
		};
		assert_taken_by(bare_flow(&catalog, &mut txn, namespace.id(), "q", view_only), "queue");
	}

	#[test]
	fn a_flow_name_is_refused_for_a_table() {
		// Without the flow probe a table takes the name of a flow; RQL cannot reach this order.
		let mut txn = create_test_admin_transaction();
		let namespace = create_namespace(&mut txn, "test");
		create_flow(&mut txn, "test", "x");

		let result = Catalog::testing().create_table(&mut txn, table_to_create("x", namespace.id()));

		assert_taken_by(result, "flow");
	}

	#[test]
	fn a_handler_name_is_refused_for_a_table_but_not_for_a_procedure() {
		// Without the handler probe, or with handler and procedure not skipping each other, the on-purpose pair
		// breaks.
		let mut txn = create_test_admin_transaction();
		let namespace = create_namespace(&mut txn, "test");
		let catalog = Catalog::testing();
		catalog.create_handler(
			&mut txn,
			crate::catalog::handler::HandlerToCreate {
				name: Fragment::internal("x"),
				namespace: namespace.id(),
				variant: handler_variant(),
				body_source: String::new(),
			},
		)
		.expect("the first handler is created");

		assert_taken_by(catalog.create_table(&mut txn, table_to_create("x", namespace.id())), "handler");
		catalog.create_procedure(&mut txn, rql_procedure("x", namespace.id()))
			.expect("a handler's procedure shares its name");

		let second = create_namespace(&mut txn, "second");
		create_table(&mut txn, "second", "y", &[]);
		let result = catalog.create_handler(
			&mut txn,
			crate::catalog::handler::HandlerToCreate {
				name: Fragment::internal("y"),
				namespace: second.id(),
				variant: handler_variant(),
				body_source: String::new(),
			},
		);
		assert_taken_by(result, "table");
	}

	#[test]
	fn a_flow_name_is_refused_for_a_transactional_view() {
		// Without the check in create_transactional_view the view is made under a bare flow's name and only its
		// own flow create fails, late and with another code.
		let mut txn = create_test_admin_transaction();
		let namespace = create_namespace(&mut txn, "test");
		create_flow(&mut txn, "test", "x");
		let result = Catalog::testing().create_transactional_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("x"),
				namespace: namespace.id(),
				columns: vec![],
				storage: ViewStorage::default(),
				sort: vec![],
			},
		);
		assert_taken_by(result, "flow");
	}
}
