// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "aggregate.rs"]
mod aggregate;
#[path = "apply_unknown_operator.rs"]
mod apply_unknown_operator;
#[path = "bulk_insert.rs"]
mod bulk_insert;
#[path = "call_argument_named_like_a_type.rs"]
mod call_argument_named_like_a_type;
#[path = "callable_call_policy.rs"]
mod callable_call_policy;
#[path = "cast_arbitrary_precision_text_round_trip.rs"]
mod cast_arbitrary_precision_text_round_trip;
#[path = "cast_arbitrary_precision_to_text.rs"]
mod cast_arbitrary_precision_to_text;
#[path = "cast_list_to_text.rs"]
mod cast_list_to_text;
#[path = "cast_target_diagnostics.rs"]
mod cast_target_diagnostics;

#[path = "closure.rs"]
mod closure;
#[path = "column_type_parameters.rs"]
mod column_type_parameters;
#[path = "digest_column_ddl.rs"]
mod digest_column_ddl;
#[path = "digest_column_write.rs"]
mod digest_column_write;
#[path = "digest_query.rs"]
mod digest_query;
#[path = "digest_row_storage.rs"]
mod digest_row_storage;
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
#[path = "inline_data_insert_target.rs"]
mod inline_data_insert_target;
#[path = "inline_data_value_types.rs"]
mod inline_data_value_types;
#[path = "insert_undefined_value.rs"]
mod insert_undefined_value;
#[path = "interceptor.rs"]
mod interceptor;
#[path = "join_digest_key.rs"]
mod join_digest_key;
#[path = "join_natural.rs"]
mod join_natural;
#[path = "join_retention_ddl.rs"]
mod join_retention_ddl;
#[path = "join_using_unknown_key_column.rs"]
mod join_using_unknown_key_column;
#[path = "memory_limit.rs"]
mod memory_limit;
#[path = "missing_column_lookup.rs"]
mod missing_column_lookup;
#[path = "nested_option_types.rs"]
mod nested_option_types;
#[path = "nested_read_policy_bypass.rs"]
mod nested_read_policy_bypass;
#[path = "none_operand_expression_types.rs"]
mod none_operand_expression_types;
#[path = "plan_cache.rs"]
mod plan_cache;
#[path = "positional_insert.rs"]
mod positional_insert;
#[path = "procedure_named_like_a_builtin.rs"]
mod procedure_named_like_a_builtin;
#[path = "procedure_param_coercion.rs"]
mod procedure_param_coercion;
#[path = "procedure_read_policy_bypass.rs"]
mod procedure_read_policy_bypass;
#[path = "procedure_shadowing_type_arguments.rs"]
mod procedure_shadowing_type_arguments;
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
#[path = "series_scan_column_types.rs"]
mod series_scan_column_types;
#[path = "series_tag_constructor_field.rs"]
mod series_tag_constructor_field;
#[path = "sort_unorderable.rs"]
mod sort_unorderable;
#[path = "system_reserved_ids.rs"]
mod system_reserved_ids;
#[path = "take.rs"]
mod take;
#[path = "time_column_undeclared.rs"]
mod time_column_undeclared;
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
#[path = "window_batch_query_fragment.rs"]
mod window_batch_query_fragment;
#[path = "write_policy_old_row.rs"]
mod write_policy_old_row;
