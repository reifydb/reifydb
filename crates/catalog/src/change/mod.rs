// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Catalog change handlers: one per object kind, decoding a CDC record describing a catalog
//! mutation, folding it into the materialised catalog view, and emitting the catalog change event
//! the rest of the workspace listens for.

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::cdc::SystemChange,
	key::{Key, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use crate::{Result, catalog::Catalog, error::CatalogChangeError};

mod binding;
mod column;
mod config;
mod dictionary;
mod flow;
mod flow_edge;
mod granted_role;
mod handler;
mod identity;
mod identity_attribute;
mod identity_attribute_value;
mod migration;
mod namespace;
mod operator;
mod operator_settings;
mod passthrough;
mod policy;
mod primary_key;
mod procedure;
mod procedure_param;
mod queue;
mod relationship;
mod ringbuffer;
mod row_settings;
mod row_shape;
mod series;
mod sink;
mod source;
mod sumtype;
mod table;
mod view;

mod role;

use binding::BindingApplier;
use column::ColumnApplier;
use config::ConfigApplier;
use dictionary::DictionaryApplier;
use flow::FlowApplier;
use flow_edge::FlowEdgeApplier;
use granted_role::GrantedRoleApplier;
use handler::HandlerApplier;
use identity::IdentityApplier;
use identity_attribute::IdentityAttributeApplier;
use identity_attribute_value::IdentityAttributeValueApplier;
use migration::{MigrationApplier, MigrationEventApplier};
use namespace::NamespaceApplier;
use operator::OperatorApplier;
use operator_settings::OperatorSettingsApplier;
use passthrough::PassthroughApplier;
use policy::PolicyApplier;
use primary_key::PrimaryKeyApplier;
use procedure::ProcedureApplier;
use procedure_param::ProcedureParamApplier;
use queue::QueueApplier;
use relationship::RelationshipApplier;
use ringbuffer::RingBufferApplier;
use role::RoleApplier;
use row_settings::RowSettingsApplier;
use row_shape::{RowShapeFieldApplier, RowShapeHeaderApplier};
use series::SeriesApplier;
use sink::SinkApplier;
use source::SourceApplier;
use sumtype::SumTypeApplier;
use table::TableApplier;
use view::ViewApplier;

pub trait CatalogChangeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()>;

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()>;
}

pub fn apply_system_change(catalog: &Catalog, txn: &mut Transaction<'_>, change: &SystemChange) -> Result<()> {
	let kind = match Key::kind(change.key()) {
		Some(k) => k,
		None => {
			return Err(CatalogChangeError::UnrecognizedKey {
				raw: change.key().as_ref().to_vec(),
			}
			.into());
		}
	};

	match kind {
		KeyKind::Binding => dispatch::<BindingApplier>(catalog, txn, change),
		KeyKind::Namespace => dispatch::<NamespaceApplier>(catalog, txn, change),
		KeyKind::Table => dispatch::<TableApplier>(catalog, txn, change),
		KeyKind::View => dispatch::<ViewApplier>(catalog, txn, change),
		KeyKind::PrimaryKey => dispatch::<PrimaryKeyApplier>(catalog, txn, change),
		KeyKind::Relationship => dispatch::<RelationshipApplier>(catalog, txn, change),
		KeyKind::Flow => dispatch::<FlowApplier>(catalog, txn, change),
		KeyKind::Operator => dispatch::<OperatorApplier>(catalog, txn, change),
		KeyKind::FlowEdge => dispatch::<FlowEdgeApplier>(catalog, txn, change),
		KeyKind::Handler => dispatch::<HandlerApplier>(catalog, txn, change),
		KeyKind::Dictionary => dispatch::<DictionaryApplier>(catalog, txn, change),
		KeyKind::SumType => dispatch::<SumTypeApplier>(catalog, txn, change),
		KeyKind::RingBuffer => dispatch::<RingBufferApplier>(catalog, txn, change),
		KeyKind::Queue => dispatch::<QueueApplier>(catalog, txn, change),
		KeyKind::Identity => dispatch::<IdentityApplier>(catalog, txn, change),
		KeyKind::Role => dispatch::<RoleApplier>(catalog, txn, change),
		KeyKind::GrantedRole => dispatch::<GrantedRoleApplier>(catalog, txn, change),
		KeyKind::IdentityAttribute => dispatch::<IdentityAttributeApplier>(catalog, txn, change),
		KeyKind::IdentityAttributeValue => dispatch::<IdentityAttributeValueApplier>(catalog, txn, change),
		KeyKind::Policy => dispatch::<PolicyApplier>(catalog, txn, change),
		KeyKind::Source => dispatch::<SourceApplier>(catalog, txn, change),
		KeyKind::Sink => dispatch::<SinkApplier>(catalog, txn, change),
		KeyKind::Migration => dispatch::<MigrationApplier>(catalog, txn, change),
		KeyKind::MigrationEvent => dispatch::<MigrationEventApplier>(catalog, txn, change),
		KeyKind::ConfigStorage => dispatch::<ConfigApplier>(catalog, txn, change),
		KeyKind::Series => dispatch::<SeriesApplier>(catalog, txn, change),
		KeyKind::RowSettings => dispatch::<RowSettingsApplier>(catalog, txn, change),
		KeyKind::OperatorSettings => dispatch::<OperatorSettingsApplier>(catalog, txn, change),
		KeyKind::RowShape => dispatch::<RowShapeHeaderApplier>(catalog, txn, change),
		KeyKind::RowShapeField => dispatch::<RowShapeFieldApplier>(catalog, txn, change),

		KeyKind::Procedure => dispatch::<ProcedureApplier>(catalog, txn, change),
		KeyKind::ProcedureParam => dispatch::<ProcedureParamApplier>(catalog, txn, change),

		KeyKind::Column | KeyKind::Columns => dispatch::<ColumnApplier>(catalog, txn, change),

		KeyKind::NamespaceTable
		| KeyKind::NamespaceView
		| KeyKind::NamespaceFlow
		| KeyKind::NamespaceRingBuffer
		| KeyKind::NamespaceQueue
		| KeyKind::NamespaceDictionary
		| KeyKind::NamespaceSumType
		| KeyKind::NamespaceHandler
		| KeyKind::NamespaceBinding
		| KeyKind::NamespaceProcedure
		| KeyKind::NamespaceSource
		| KeyKind::NamespaceSink
		| KeyKind::NamespaceSeries
		| KeyKind::VariantHandler
		| KeyKind::PolicyOp => dispatch::<PassthroughApplier>(catalog, txn, change),

		_ => dispatch::<PassthroughApplier>(catalog, txn, change),
	}
}

fn dispatch<T: CatalogChangeApplier>(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	change: &SystemChange,
) -> Result<()> {
	match change {
		SystemChange::Insert {
			key,
			post,
		} => T::set(catalog, txn, key, post),
		SystemChange::Update {
			key,
			post,
			..
		} => T::set(catalog, txn, key, post),
		SystemChange::Delete {
			key,
			..
		} => T::remove(catalog, txn, key),
	}
}
