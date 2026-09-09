// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "bulk_insert.rs"]
mod bulk_insert;
#[path = "callable_call_policy.rs"]
mod callable_call_policy;
#[path = "cast_target_diagnostics.rs"]
mod cast_target_diagnostics;

#[path = "closure.rs"]
mod closure;
#[path = "empty_batch_system_columns.rs"]
mod empty_batch_system_columns;
#[path = "empty_result_column_types.rs"]
mod empty_result_column_types;
#[path = "events.rs"]
mod events;
#[path = "flow_dag_load.rs"]
mod flow_dag_load;
#[path = "identity_dml_context.rs"]
mod identity_dml_context;
#[path = "identity_inject_escalation.rs"]
mod identity_inject_escalation;
#[path = "identity_kind.rs"]
mod identity_kind;
#[path = "interceptor.rs"]
mod interceptor;
#[path = "join_retention_ddl.rs"]
mod join_retention_ddl;
#[path = "memory_limit.rs"]
mod memory_limit;
#[path = "plan_cache.rs"]
mod plan_cache;
#[path = "positional_insert.rs"]
mod positional_insert;
#[path = "procedure_param_coercion.rs"]
mod procedure_param_coercion;
#[path = "procedure_read_policy_bypass.rs"]
mod procedure_read_policy_bypass;
#[path = "queue/main.rs"]
mod queue;
#[path = "read_only.rs"]
mod read_only;
#[path = "returning.rs"]
mod returning;
#[path = "ringbuffer.rs"]
mod ringbuffer;
#[path = "shutdown.rs"]
mod shutdown;
#[path = "system_reserved_ids.rs"]
mod system_reserved_ids;
#[path = "take.rs"]
mod take;
#[path = "time_domain_chained_view.rs"]
mod time_domain_chained_view;
#[path = "time_domain_recheck.rs"]
mod time_domain_recheck;
#[path = "time_update_lifecycle.rs"]
mod time_update_lifecycle;
#[path = "top_k_system_columns.rs"]
mod top_k_system_columns;
#[path = "transaction.rs"]
mod transaction;
#[path = "transactional_view_unimplemented.rs"]
mod transactional_view_unimplemented;
#[path = "udf.rs"]
mod udf;
#[path = "virtual_table_diagnostics.rs"]
mod virtual_table_diagnostics;
