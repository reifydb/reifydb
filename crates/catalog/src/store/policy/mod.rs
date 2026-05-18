// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
	let row = multi.row;
	let id = policy::SHAPE.get_u64(&row, policy::ID);
	let name_str = policy::SHAPE.get_utf8(&row, policy::NAME).to_string();
	let name = if name_str.is_empty() {
		None
	} else {
		Some(name_str)
	};
	let target_type_str = policy::SHAPE.get_utf8(&row, policy::TARGET_TYPE);
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
	let target_ns_str = policy::SHAPE.get_utf8(&row, policy::TARGET_NAMESPACE).to_string();
	let target_namespace = if target_ns_str.is_empty() {
		None
	} else {
		Some(target_ns_str)
	};
	let target_shape_str = policy::SHAPE.get_utf8(&row, policy::TARGET_SHAPE).to_string();
	let target_shape = if target_shape_str.is_empty() {
		None
	} else {
		Some(target_shape_str)
	};
	let enabled = policy::SHAPE.get_bool(&row, policy::ENABLED);

	Policy {
		id,
		name,
		target_type,
		target_namespace,
		target_shape,
		enabled,
	}
}

pub(crate) fn convert_policy_op(multi: MultiVersionRow) -> PolicyOperation {
	let row = multi.row;
	let policy_id = policy_op::SHAPE.get_u64(&row, policy_op::POLICY_ID);
	let operation = policy_op::SHAPE.get_utf8(&row, policy_op::OPERATION).to_string();
	let body_source = policy_op::SHAPE.get_utf8(&row, policy_op::BODY_SOURCE).to_string();

	PolicyOperation {
		policy_id,
		operation,
		body_source,
	}
}
