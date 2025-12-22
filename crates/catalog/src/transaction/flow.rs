// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	interface::{FlowDef, FlowId, NamespaceId, QueryTransaction},
	return_error,
};
use reifydb_type::{Fragment, diagnostic::catalog::flow_not_found};
use tracing::instrument;

use crate::{CatalogStore, transaction::MaterializedCatalogTransaction};

#[async_trait(?Send)]
pub trait CatalogFlowQueryOperations: Send {
	async fn find_flow(&mut self, id: FlowId) -> crate::Result<Option<FlowDef>>;

	async fn find_flow_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<Option<FlowDef>>;

	async fn get_flow(&mut self, id: FlowId) -> crate::Result<FlowDef>;

	async fn get_flow_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<FlowDef>;
}

#[async_trait(?Send)]
impl<QT: QueryTransaction + MaterializedCatalogTransaction + Send> CatalogFlowQueryOperations for QT {
	#[instrument(name = "catalog::flow::find", level = "trace", skip(self))]
	async fn find_flow(&mut self, id: FlowId) -> crate::Result<Option<FlowDef>> {
		CatalogStore::find_flow(self, id).await
	}

	#[instrument(name = "catalog::flow::find_by_name", level = "trace", skip(self, name))]
	async fn find_flow_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<Option<FlowDef>> {
		let name = name.into();
		CatalogStore::find_flow_by_name(self, namespace, name.text()).await
	}

	#[instrument(name = "catalog::flow::get", level = "trace", skip(self))]
	async fn get_flow(&mut self, id: FlowId) -> crate::Result<FlowDef> {
		CatalogStore::get_flow(self, id).await
	}

	#[instrument(name = "catalog::flow::get_by_name", level = "trace", skip(self, name))]
	async fn get_flow_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<FlowDef> {
		let name = name.into();
		let name_text = name.text().to_string();
		let flow = self.find_flow_by_name(namespace, name.clone()).await?;
		match flow {
			Some(f) => Ok(f),
			None => {
				let namespace = CatalogStore::get_namespace(self, namespace).await?;
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
