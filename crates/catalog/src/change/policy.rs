// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::catalog::policy::{Policy, PolicyTargetType},
	key::{EncodableKey, kind::KeyKind, policy::PolicyKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::policy::shape::policy};

pub(super) struct PolicyApplier;

impl CatalogChangeApplier for PolicyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let p = decode_policy(bytes);
		catalog.cache.set_policy(p.id, txn.version(), Some(p));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = PolicyKey::decode(key).map(|k| k.policy).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Policy,
		})?;
		catalog.cache.set_policy(id, txn.version(), None);
		Ok(())
	}
}

fn decode_policy(bytes: &EncodedBytes) -> Policy {
	let id = policy::get_id(bytes);
	let name_str = policy::get_name(bytes).to_string();
	let name = if name_str.is_empty() {
		None
	} else {
		Some(name_str)
	};
	let target_type_str = policy::get_target_type(bytes);
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
	let target_ns_str = policy::get_target_namespace(bytes).to_string();
	let target_namespace = if target_ns_str.is_empty() {
		None
	} else {
		Some(target_ns_str)
	};
	let target_object_str = policy::get_target_object(bytes).to_string();
	let target_object = if target_object_str.is_empty() {
		None
	} else {
		Some(target_object_str)
	};
	let enabled = policy::get_enabled(bytes);

	Policy {
		id,
		name,
		target_type,
		target_namespace,
		target_object,
		enabled,
	}
}
