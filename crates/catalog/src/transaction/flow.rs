// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{FlowDef, FlowId, NamespaceId, QueryTransaction},
	return_error,
};
use reifydb_type::{IntoFragment, diagnostic::catalog::flow_not_found};
use tracing::instrument;

use crate::{CatalogStore, transaction::MaterializedCatalogTransaction};

pub trait CatalogFlowQueryOperations {
	fn find_flow(&mut self, id: FlowId) -> crate::Result<Option<FlowDef>>;

	fn find_flow_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<FlowDef>>;

	fn get_flow(&mut self, id: FlowId) -> crate::Result<FlowDef>;

	fn get_flow_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<FlowDef>;
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction> CatalogFlowQueryOperations for QT {
	#[instrument(name = "catalog::flow::find", level = "trace", skip(self))]
	fn find_flow(&mut self, id: FlowId) -> crate::Result<Option<FlowDef>> {
		CatalogStore::find_flow(self, id)
	}

	#[instrument(name = "catalog::flow::find_by_name", level = "trace", skip(self, name))]
	fn find_flow_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<FlowDef>> {
		let name = name.into_fragment();
		CatalogStore::find_flow_by_name(self, namespace, name.text())
	}

	#[instrument(name = "catalog::flow::get", level = "trace", skip(self))]
	fn get_flow(&mut self, id: FlowId) -> crate::Result<FlowDef> {
		CatalogStore::get_flow(self, id)
	}

	#[instrument(name = "catalog::flow::get_by_name", level = "trace", skip(self, name))]
	fn get_flow_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<FlowDef> {
		let name = name.into_fragment();
		let name_text = name.text().to_string();
		let flow = self.find_flow_by_name(namespace, name.clone())?;
		match flow {
			Some(f) => Ok(f),
			None => {
				let namespace = CatalogStore::get_namespace(self, namespace)?;
				return_error!(flow_not_found(name, &namespace.name, &name_text))
			}
		}
	}
}

pub trait CatalogTrackFlowChangeOperations {
	fn track_flow_def_created(&mut self, flow: FlowDef) -> crate::Result<()>;

	fn track_flow_def_updated(&mut self, pre: FlowDef, post: FlowDef) -> crate::Result<()>;

	fn track_flow_def_deleted(&mut self, flow: FlowDef) -> crate::Result<()>;
}
