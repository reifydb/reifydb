// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::policy::{Policy, PolicyTargetType},
	key::{EncodableKey, kind::KeyKind, policy::PolicyKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::policy::schema::policy};

pub(super) struct PolicyApplier;

impl CatalogChangeApplier for PolicyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let p = decode_policy(row);
		catalog.materialized.set_policy(p.id, txn.version(), Some(p));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = PolicyKey::decode(key).map(|k| k.policy).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Policy,
		})?;
		catalog.materialized.set_policy(id, txn.version(), None);
		Ok(())
	}
}

fn decode_policy(row: &EncodedRow) -> Policy {
	let id = policy::SCHEMA.get_u64(row, policy::ID);
	let name_str = policy::SCHEMA.get_utf8(row, policy::NAME).to_string();
	let name = if name_str.is_empty() {
		None
	} else {
		Some(name_str)
	};
	let target_type_str = policy::SCHEMA.get_utf8(row, policy::TARGET_TYPE);
	let target_type = match target_type_str {
		"table" => PolicyTargetType::Table,
		"column" => PolicyTargetType::Column,
		"namespace" => PolicyTargetType::Namespace,
		"procedure" => PolicyTargetType::Procedure,
		"function" => PolicyTargetType::Function,
		"subscription" => PolicyTargetType::Subscription,
		"series" => PolicyTargetType::Series,
		"dictionary" => PolicyTargetType::Dictionary,
		"session" => PolicyTargetType::Session,
		"feature" => PolicyTargetType::Feature,
		"view" => PolicyTargetType::View,
		"ringbuffer" => PolicyTargetType::RingBuffer,
		_ => PolicyTargetType::Table,
	};
	let target_ns_str = policy::SCHEMA.get_utf8(row, policy::TARGET_NAMESPACE).to_string();
	let target_namespace = if target_ns_str.is_empty() {
		None
	} else {
		Some(target_ns_str)
	};
	let target_obj_str = policy::SCHEMA.get_utf8(row, policy::TARGET_OBJECT).to_string();
	let target_object = if target_obj_str.is_empty() {
		None
	} else {
		Some(target_obj_str)
	};
	let enabled = policy::SCHEMA.get_bool(row, policy::ENABLED);

	Policy {
		id,
		name,
		target_type,
		target_namespace,
		target_object,
		enabled,
	}
}
