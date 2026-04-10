// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::SystemChange,
	key::{Key, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use crate::{Result, catalog::Catalog, error::CatalogChangeError};

mod column;
mod config;
mod dictionary;
mod flow;
mod granted_role;
mod handler;
mod identity;
mod migration;
mod namespace;
mod passthrough;
mod policy;
mod primary_key;
mod retention;
mod ringbuffer;
mod row_shape;
mod series;
mod sink;
mod source;
mod sumtype;
mod table;
mod ttl;
mod view;

mod role;

use column::ColumnApplier;
use config::ConfigApplier;
use dictionary::DictionaryApplier;
use flow::FlowApplier;
use granted_role::GrantedRoleApplier;
use handler::HandlerApplier;
use identity::IdentityApplier;
use migration::{MigrationApplier, MigrationEventApplier};
use namespace::NamespaceApplier;
use passthrough::PassthroughApplier;
use policy::PolicyApplier;
use primary_key::PrimaryKeyApplier;
use retention::{OperatorRetentionStrategyApplier, ShapeRetentionStrategyApplier};
use ringbuffer::RingBufferApplier;
use role::RoleApplier;
use row_shape::{RowShapeFieldApplier, RowShapeHeaderApplier};
use series::SeriesApplier;
use sink::SinkApplier;
use source::SourceApplier;
use sumtype::SumTypeApplier;
use table::TableApplier;
use ttl::RowTtlApplier;
use view::ViewApplier;

pub trait CatalogChangeApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()>;

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
		KeyKind::Namespace => dispatch::<NamespaceApplier>(catalog, txn, change),
		KeyKind::Table => dispatch::<TableApplier>(catalog, txn, change),
		KeyKind::View => dispatch::<ViewApplier>(catalog, txn, change),
		KeyKind::PrimaryKey => dispatch::<PrimaryKeyApplier>(catalog, txn, change),
		KeyKind::Flow => dispatch::<FlowApplier>(catalog, txn, change),
		KeyKind::Handler => dispatch::<HandlerApplier>(catalog, txn, change),
		KeyKind::Dictionary => dispatch::<DictionaryApplier>(catalog, txn, change),
		KeyKind::SumType => dispatch::<SumTypeApplier>(catalog, txn, change),
		KeyKind::RingBuffer => dispatch::<RingBufferApplier>(catalog, txn, change),
		KeyKind::Identity => dispatch::<IdentityApplier>(catalog, txn, change),
		KeyKind::Role => dispatch::<RoleApplier>(catalog, txn, change),
		KeyKind::GrantedRole => dispatch::<GrantedRoleApplier>(catalog, txn, change),
		KeyKind::Policy => dispatch::<PolicyApplier>(catalog, txn, change),
		KeyKind::Source => dispatch::<SourceApplier>(catalog, txn, change),
		KeyKind::Sink => dispatch::<SinkApplier>(catalog, txn, change),
		KeyKind::Migration => dispatch::<MigrationApplier>(catalog, txn, change),
		KeyKind::MigrationEvent => dispatch::<MigrationEventApplier>(catalog, txn, change),
		KeyKind::ConfigStorage => dispatch::<ConfigApplier>(catalog, txn, change),
		KeyKind::Series => dispatch::<SeriesApplier>(catalog, txn, change),
		KeyKind::ShapeRetentionStrategy => dispatch::<ShapeRetentionStrategyApplier>(catalog, txn, change),
		KeyKind::OperatorRetentionStrategy => {
			dispatch::<OperatorRetentionStrategyApplier>(catalog, txn, change)
		}
		KeyKind::RowTtl => dispatch::<RowTtlApplier>(catalog, txn, change),

		KeyKind::Shape => dispatch::<RowShapeHeaderApplier>(catalog, txn, change),
		KeyKind::RowShapeField => dispatch::<RowShapeFieldApplier>(catalog, txn, change),

		KeyKind::Column | KeyKind::Columns => dispatch::<ColumnApplier>(catalog, txn, change),

		// Secondary index keys — write to txn, no materialized catalog action
		KeyKind::NamespaceTable
		| KeyKind::NamespaceView
		| KeyKind::NamespaceFlow
		| KeyKind::NamespaceRingBuffer
		| KeyKind::NamespaceDictionary
		| KeyKind::NamespaceSumType
		| KeyKind::NamespaceHandler
		| KeyKind::NamespaceSource
		| KeyKind::NamespaceSink
		| KeyKind::NamespaceSeries
		| KeyKind::VariantHandler
		| KeyKind::PolicyOp => dispatch::<PassthroughApplier>(catalog, txn, change),

		// All other keys (Row data, etc.) — write to txn, no materialized catalog action
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
