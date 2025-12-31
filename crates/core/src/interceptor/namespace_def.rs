// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::marker::PhantomData;

use crate::{define_api_function, define_closure_interceptor, define_interceptor, interface::NamespaceDef};

// NAMESPACE POST CREATE
define_interceptor!(
    context: NamespaceDefPostCreateContext<T>,
    trait: NamespaceDefPostCreateInterceptor,
    fields: {
	txn: &'a mut T,
	post: &'a NamespaceDef}
);

define_closure_interceptor!(
	ClosureNamespaceDefPostCreateInterceptor,
	NamespaceDefPostCreateInterceptor,
	NamespaceDefPostCreateContext,
	with_transaction
);

define_api_function!(
    namespace_def_post_create,
    ClosureNamespaceDefPostCreateInterceptor<T, F>,
    NamespaceDefPostCreateContext<T>
);

// NAMESPACE PRE UPDATE
define_interceptor!(
    context: NamespaceDefPreUpdateContext<T>,
    trait: NamespaceDefPreUpdateInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a NamespaceDef}
);

define_closure_interceptor!(
	ClosureNamespaceDefPreUpdateInterceptor,
	NamespaceDefPreUpdateInterceptor,
	NamespaceDefPreUpdateContext,
	with_transaction
);

define_api_function!(
    namespace_def_pre_update,
    ClosureNamespaceDefPreUpdateInterceptor<T, F>,
    NamespaceDefPreUpdateContext<T>
);

// NAMESPACE POST UPDATE
define_interceptor!(
    context: NamespaceDefPostUpdateContext<T>,
    trait: NamespaceDefPostUpdateInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a NamespaceDef,
	post: &'a NamespaceDef}
);

define_closure_interceptor!(
	ClosureNamespaceDefPostUpdateInterceptor,
	NamespaceDefPostUpdateInterceptor,
	NamespaceDefPostUpdateContext,
	with_transaction
);

define_api_function!(
    namespace_def_post_update,
    ClosureNamespaceDefPostUpdateInterceptor<T, F>,
    NamespaceDefPostUpdateContext<T>
);

// NAMESPACE PRE DELETE
define_interceptor!(
    context: NamespaceDefPreDeleteContext<T>,
    trait: NamespaceDefPreDeleteInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a NamespaceDef}
);

define_closure_interceptor!(
	ClosureNamespaceDefPreDeleteInterceptor,
	NamespaceDefPreDeleteInterceptor,
	NamespaceDefPreDeleteContext,
	with_transaction
);

define_api_function!(
    namespace_def_pre_delete,
    ClosureNamespaceDefPreDeleteInterceptor<T, F>,
    NamespaceDefPreDeleteContext<T>
);

use crate::impl_register_interceptor;

impl_register_interceptor!(
    ClosureNamespaceDefPostCreateInterceptor<T, F>,
    NamespaceDefPostCreateContext<T>,
    NamespaceDefPostCreateInterceptor,
    namespace_def_post_create
);

impl_register_interceptor!(
    ClosureNamespaceDefPreUpdateInterceptor<T, F>,
    NamespaceDefPreUpdateContext<T>,
    NamespaceDefPreUpdateInterceptor,
    namespace_def_pre_update
);

impl_register_interceptor!(
    ClosureNamespaceDefPostUpdateInterceptor<T, F>,
    NamespaceDefPostUpdateContext<T>,
    NamespaceDefPostUpdateInterceptor,
    namespace_def_post_update
);

impl_register_interceptor!(
    ClosureNamespaceDefPreDeleteInterceptor<T, F>,
    NamespaceDefPreDeleteContext<T>,
    NamespaceDefPreDeleteInterceptor,
    namespace_def_pre_delete
);
