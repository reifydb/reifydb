// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::marker::PhantomData;

use crate::{define_api_function, define_closure_interceptor, define_interceptor, interface::TableDef};

// TABLE POST CREATE
define_interceptor!(
    context: TableDefPostCreateContext<T>,
    trait: TableDefPostCreateInterceptor,
    fields: {
	txn: &'a mut T,
	post: &'a TableDef}
);

define_closure_interceptor!(
	ClosureTableDefPostCreateInterceptor,
	TableDefPostCreateInterceptor,
	TableDefPostCreateContext,
	with_transaction
);

define_api_function!(
    table_def_post_create,
    ClosureTableDefPostCreateInterceptor<T, F>,
    TableDefPostCreateContext<T>
);

// TABLE PRE UPDATE
define_interceptor!(
    context: TableDefPreUpdateContext<T>,
    trait: TableDefPreUpdateInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a TableDef}
);

define_closure_interceptor!(
	ClosureTableDefPreUpdateInterceptor,
	TableDefPreUpdateInterceptor,
	TableDefPreUpdateContext,
	with_transaction
);

define_api_function!(
    table_def_pre_update,
    ClosureTableDefPreUpdateInterceptor<T, F>,
    TableDefPreUpdateContext<T>
);

// TABLE POST UPDATE
define_interceptor!(
    context: TableDefPostUpdateContext<T>,
    trait: TableDefPostUpdateInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a TableDef,
	post: &'a TableDef}
);

define_closure_interceptor!(
	ClosureTableDefPostUpdateInterceptor,
	TableDefPostUpdateInterceptor,
	TableDefPostUpdateContext,
	with_transaction
);

define_api_function!(
    table_def_post_update,
    ClosureTableDefPostUpdateInterceptor<T, F>,
    TableDefPostUpdateContext<T>
);

// TABLE PRE DELETE
define_interceptor!(
    context: TableDefPreDeleteContext<T>,
    trait: TableDefPreDeleteInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a TableDef}
);

define_closure_interceptor!(
	ClosureTableDefPreDeleteInterceptor,
	TableDefPreDeleteInterceptor,
	TableDefPreDeleteContext,
	with_transaction
);

define_api_function!(
    table_def_pre_delete,
    ClosureTableDefPreDeleteInterceptor<T, F>,
    TableDefPreDeleteContext<T>
);

use crate::impl_register_interceptor;

impl_register_interceptor!(
    ClosureTableDefPostCreateInterceptor<T, F>,
    TableDefPostCreateContext<T>,
    TableDefPostCreateInterceptor,
    table_def_post_create
);

impl_register_interceptor!(
    ClosureTableDefPreUpdateInterceptor<T, F>,
    TableDefPreUpdateContext<T>,
    TableDefPreUpdateInterceptor,
    table_def_pre_update
);

impl_register_interceptor!(
    ClosureTableDefPostUpdateInterceptor<T, F>,
    TableDefPostUpdateContext<T>,
    TableDefPostUpdateInterceptor,
    table_def_post_update
);

impl_register_interceptor!(
    ClosureTableDefPreDeleteInterceptor<T, F>,
    TableDefPreDeleteContext<T>,
    TableDefPreDeleteInterceptor,
    table_def_pre_delete
);
