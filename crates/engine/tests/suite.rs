// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "aggregate.rs"]
mod aggregate;
#[path = "aggregate_arguments.rs"]
mod aggregate_arguments;
#[path = "aggregate_invalid_argument_fragment.rs"]
mod aggregate_invalid_argument_fragment;
#[path = "append_batch_query.rs"]
mod append_batch_query;
#[path = "apply_unknown_operator.rs"]
mod apply_unknown_operator;
#[path = "arithmetic_with_none_in_conditional.rs"]
mod arithmetic_with_none_in_conditional;
#[path = "assert_scalar_series_compare.rs"]
mod assert_scalar_series_compare;
#[path = "assert_short_boolean_result.rs"]
mod assert_short_boolean_result;
#[path = "builtin_arity_before_arguments.rs"]
mod builtin_arity_before_arguments;
#[path = "bulk_insert.rs"]
mod bulk_insert;
#[path = "bulk_insert_mixed_value_types.rs"]
mod bulk_insert_mixed_value_types;
#[path = "call_argument_named_like_a_type.rs"]
mod call_argument_named_like_a_type;
#[path = "call_argument_undefined_variable.rs"]
mod call_argument_undefined_variable;
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
#[path = "closure_parameter_type_arguments.rs"]
mod closure_parameter_type_arguments;
#[path = "closure_value_as_rows.rs"]
mod closure_value_as_rows;
#[path = "column_type_parameters.rs"]
mod column_type_parameters;
#[path = "dictionary_insert_value_type.rs"]
mod dictionary_insert_value_type;
#[path = "dictionary_update_to_none.rs"]
mod dictionary_update_to_none;
#[path = "digest_aggregate.rs"]
mod digest_aggregate;
#[path = "digest_column_ddl.rs"]
mod digest_column_ddl;
#[path = "digest_column_write.rs"]
mod digest_column_write;
#[path = "digest_query.rs"]
mod digest_query;
#[path = "digest_row_storage.rs"]
mod digest_row_storage;
#[path = "distinct_over_negative_zero.rs"]
mod distinct_over_negative_zero;
#[path = "distinct_row_identity.rs"]
mod distinct_row_identity;
#[path = "distinct_typed_key.rs"]
mod distinct_typed_key;
#[path = "division_by_a_none_divisor.rs"]
mod division_by_a_none_divisor;
#[path = "duration_add_overflow.rs"]
mod duration_add_overflow;
#[path = "duration_mixed_unit_identity.rs"]
mod duration_mixed_unit_identity;
#[path = "duration_subtract_and_scale_overflow.rs"]
mod duration_subtract_and_scale_overflow;
#[path = "empty_batch_system_columns.rs"]
mod empty_batch_system_columns;
#[path = "empty_result_column_types.rs"]
mod empty_result_column_types;
#[path = "empty_result_rownum.rs"]
mod empty_result_rownum;
#[path = "enum_column_optional.rs"]
mod enum_column_optional;
#[path = "enum_variant_in_expression.rs"]
mod enum_variant_in_expression;
#[path = "enum_variant_in_query.rs"]
mod enum_variant_in_query;
#[path = "enum_variant_update_errors.rs"]
mod enum_variant_update_errors;
#[path = "events.rs"]
mod events;
#[path = "extend_duplicate_column_fragment.rs"]
mod extend_duplicate_column_fragment;
#[path = "flow_dag_load.rs"]
mod flow_dag_load;
#[path = "from_default_order.rs"]
mod from_default_order;
#[path = "grouped_sum_overflow.rs"]
mod grouped_sum_overflow;
#[path = "handler_body_pipe.rs"]
mod handler_body_pipe;
#[path = "identity_dml_context.rs"]
mod identity_dml_context;
#[path = "identity_inject_escalation.rs"]
mod identity_inject_escalation;
#[path = "identity_kind.rs"]
mod identity_kind;
#[path = "in_list_with_a_none_item.rs"]
mod in_list_with_a_none_item;
#[path = "inline_column_property_ddl.rs"]
mod inline_column_property_ddl;
#[path = "inline_constructor_unknown_column.rs"]
mod inline_constructor_unknown_column;
#[path = "inline_data_insert_target.rs"]
mod inline_data_insert_target;
#[path = "inline_data_value_types.rs"]
mod inline_data_value_types;
#[path = "inline_duplicate_fields.rs"]
mod inline_duplicate_fields;
#[path = "inline_saturation_property.rs"]
mod inline_saturation_property;
#[path = "insert_piped_source_diagnostic.rs"]
mod insert_piped_source_diagnostic;
#[path = "insert_undefined_value.rs"]
mod insert_undefined_value;
#[path = "interceptor.rs"]
mod interceptor;
#[path = "join_digest_key.rs"]
mod join_digest_key;
#[path = "join_natural.rs"]
mod join_natural;
#[path = "join_output_columns.rs"]
mod join_output_columns;
#[path = "join_retention_ddl.rs"]
mod join_retention_ddl;
#[path = "join_using_condition_fragment.rs"]
mod join_using_condition_fragment;
#[path = "join_using_unknown_key_column.rs"]
mod join_using_unknown_key_column;
#[path = "list_param_insert.rs"]
mod list_param_insert;
#[path = "map_ends_in_a_comment.rs"]
mod map_ends_in_a_comment;
#[path = "memory_limit.rs"]
mod memory_limit;
#[path = "missing_column_lookup.rs"]
mod missing_column_lookup;
#[path = "nan_arithmetic.rs"]
mod nan_arithmetic;
#[path = "nested_option_param_none.rs"]
mod nested_option_param_none;
#[path = "nested_option_types.rs"]
mod nested_option_types;
#[path = "nested_read_policy_bypass.rs"]
mod nested_read_policy_bypass;
#[path = "none_operand_expression_types.rs"]
mod none_operand_expression_types;
#[path = "percentile_aggregate.rs"]
mod percentile_aggregate;
#[path = "percentile_aggregate_errors.rs"]
mod percentile_aggregate_errors;
#[path = "plan_cache.rs"]
mod plan_cache;
#[path = "positional_insert.rs"]
mod positional_insert;
#[path = "prefix_minus_integer_edges.rs"]
mod prefix_minus_integer_edges;
#[path = "prefix_minus_temporal_and_uuid.rs"]
mod prefix_minus_temporal_and_uuid;
#[path = "prefix_plus_unsigned.rs"]
mod prefix_plus_unsigned;
#[path = "procedure_body_pipe.rs"]
mod procedure_body_pipe;
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
#[path = "remainder_by_a_none_divisor.rs"]
mod remainder_by_a_none_divisor;
#[path = "returning.rs"]
mod returning;
#[path = "ringbuffer.rs"]
mod ringbuffer;
#[path = "run_tests_error_quotes_body.rs"]
mod run_tests_error_quotes_body;
#[path = "series_key_write.rs"]
mod series_key_write;
#[path = "series_scan_column_types.rs"]
mod series_scan_column_types;
#[path = "series_tag_constructor_field.rs"]
mod series_tag_constructor_field;
#[path = "series_update_tag.rs"]
mod series_update_tag;
#[path = "series_write_coercion.rs"]
mod series_write_coercion;
#[path = "shutdown.rs"]
mod shutdown;
#[path = "sort_key_order.rs"]
mod sort_key_order;
#[path = "sort_unorderable.rs"]
mod sort_unorderable;
#[path = "system_reserved_ids.rs"]
mod system_reserved_ids;
#[path = "take.rs"]
mod take;
#[path = "testing_dictionary_changed.rs"]
mod testing_dictionary_changed;
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
#[path = "udf_arity.rs"]
mod udf_arity;
#[path = "udf_arity_paths.rs"]
mod udf_arity_paths;
#[path = "udf_body_builtin_call_over_rows.rs"]
mod udf_body_builtin_call_over_rows;
#[path = "udf_declared_return_type.rs"]
mod udf_declared_return_type;
#[path = "udf_declared_return_type_paths.rs"]
mod udf_declared_return_type_paths;
#[path = "udf_mixed_return_types.rs"]
mod udf_mixed_return_types;
#[path = "udf_mixed_return_types_paths.rs"]
mod udf_mixed_return_types_paths;
#[path = "udf_parameter_type.rs"]
mod udf_parameter_type;
#[path = "udf_parameter_type_paths.rs"]
mod udf_parameter_type_paths;
#[path = "unsigned_signed_compare_edges.rs"]
mod unsigned_signed_compare_edges;
#[path = "virtual_table_diagnostics.rs"]
mod virtual_table_diagnostics;
#[path = "window_batch_query.rs"]
mod window_batch_query;
#[path = "window_batch_query_fragment.rs"]
mod window_batch_query_fragment;
#[path = "window_batch_query_paths.rs"]
mod window_batch_query_paths;
#[path = "write_policy_old_row.rs"]
mod write_policy_old_row;
