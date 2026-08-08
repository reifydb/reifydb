// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{
	catalog::policy::{Policy, PolicyOperation, PolicyTargetType},
	store::MultiVersionRow,
};

use crate::store::policy::shape::{policy, policy_op};

pub mod alter;
pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_policy(multi: MultiVersionRow) -> Policy {
	let bytes = multi.bytes;
	let id = policy::get_id(&bytes);
	let name_str = policy::get_name(&bytes).to_string();
	let name = if name_str.is_empty() {
		None
	} else {
		Some(name_str)
	};
	let target_type_str = policy::get_target_type(&bytes);
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
	let target_ns_str = policy::get_target_namespace(&bytes).to_string();
	let target_namespace = if target_ns_str.is_empty() {
		None
	} else {
		Some(target_ns_str)
	};
	let target_object_str = policy::get_target_object(&bytes).to_string();
	let target_object = if target_object_str.is_empty() {
		None
	} else {
		Some(target_object_str)
	};
	let enabled = policy::get_enabled(&bytes);

	Policy {
		id,
		name,
		target_type,
		target_namespace,
		target_object,
		enabled,
	}
}

pub(crate) fn convert_policy_op(multi: MultiVersionRow) -> PolicyOperation {
	let bytes = multi.bytes;
	let policy_id = policy_op::get_policy_id(&bytes);
	let operation = policy_op::get_operation(&bytes).to_string();
	let body_source = policy_op::get_body_source(&bytes).to_string();

	PolicyOperation {
		policy_id,
		operation,
		body_source,
	}
}
