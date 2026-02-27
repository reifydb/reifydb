// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	catalog::policy::{PolicyTargetType, SecurityPolicyDef, SecurityPolicyOperationDef},
	store::MultiVersionValues,
};

use crate::store::policy::schema::{security_policy, security_policy_op};

pub mod alter;
pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod schema;

pub(crate) fn convert_security_policy(multi: MultiVersionValues) -> SecurityPolicyDef {
	let row = multi.values;
	let id = security_policy::SCHEMA.get_u64(&row, security_policy::ID);
	let name_str = security_policy::SCHEMA.get_utf8(&row, security_policy::NAME).to_string();
	let name = if name_str.is_empty() {
		None
	} else {
		Some(name_str)
	};
	let target_type_str = security_policy::SCHEMA.get_utf8(&row, security_policy::TARGET_TYPE);
	let target_type = match target_type_str {
		"table" => PolicyTargetType::Table,
		"column" => PolicyTargetType::Column,
		"namespace" => PolicyTargetType::Namespace,
		"procedure" => PolicyTargetType::Procedure,
		"function" => PolicyTargetType::Function,
		"flow" => PolicyTargetType::Flow,
		"subscription" => PolicyTargetType::Subscription,
		"series" => PolicyTargetType::Series,
		"dictionary" => PolicyTargetType::Dictionary,
		"session" => PolicyTargetType::Session,
		"feature" => PolicyTargetType::Feature,
		_ => PolicyTargetType::Table,
	};
	let target_ns_str = security_policy::SCHEMA.get_utf8(&row, security_policy::TARGET_NAMESPACE).to_string();
	let target_namespace = if target_ns_str.is_empty() {
		None
	} else {
		Some(target_ns_str)
	};
	let target_obj_str = security_policy::SCHEMA.get_utf8(&row, security_policy::TARGET_OBJECT).to_string();
	let target_object = if target_obj_str.is_empty() {
		None
	} else {
		Some(target_obj_str)
	};
	let enabled = security_policy::SCHEMA.get_bool(&row, security_policy::ENABLED);

	SecurityPolicyDef {
		id,
		name,
		target_type,
		target_namespace,
		target_object,
		enabled,
	}
}

pub(crate) fn convert_security_policy_op(multi: MultiVersionValues) -> SecurityPolicyOperationDef {
	let row = multi.values;
	let policy_id = security_policy_op::SCHEMA.get_u64(&row, security_policy_op::POLICY_ID);
	let operation = security_policy_op::SCHEMA.get_utf8(&row, security_policy_op::OPERATION).to_string();
	let body_source = security_policy_op::SCHEMA.get_utf8(&row, security_policy_op::BODY_SOURCE).to_string();

	SecurityPolicyOperationDef {
		policy_id,
		operation,
		body_source,
	}
}
