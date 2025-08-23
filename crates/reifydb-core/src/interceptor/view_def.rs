// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use crate::{
    define_api_function, define_closure_interceptor, define_interceptor,
    interface::ViewDef,
};

// VIEW POST CREATE
define_interceptor!(
    context: ViewDefPostCreateContext<T>,
    trait: ViewDefPostCreateInterceptor,
    fields: {
        txn: &'a mut T,
        post: &'a ViewDef,
    }
);

define_closure_interceptor!(
    ClosureViewDefPostCreateInterceptor,
    ViewDefPostCreateInterceptor,
    ViewDefPostCreateContext,
    with_transaction
);

define_api_function!(
    view_def_post_create,
    ClosureViewDefPostCreateInterceptor<T, F>,
    ViewDefPostCreateContext<T>
);

// VIEW PRE UPDATE
define_interceptor!(
    context: ViewDefPreUpdateContext<T>,
    trait: ViewDefPreUpdateInterceptor,
    fields: {
        txn: &'a mut T,
        pre: &'a ViewDef,
    }
);

define_closure_interceptor!(
    ClosureViewDefPreUpdateInterceptor,
    ViewDefPreUpdateInterceptor,
    ViewDefPreUpdateContext,
    with_transaction
);

define_api_function!(
    view_def_pre_update,
    ClosureViewDefPreUpdateInterceptor<T, F>,
    ViewDefPreUpdateContext<T>
);

// VIEW POST UPDATE
define_interceptor!(
    context: ViewDefPostUpdateContext<T>,
    trait: ViewDefPostUpdateInterceptor,
    fields: {
        txn: &'a mut T,
        pre: &'a ViewDef,
        post: &'a ViewDef,
    }
);

define_closure_interceptor!(
    ClosureViewDefPostUpdateInterceptor,
    ViewDefPostUpdateInterceptor,
    ViewDefPostUpdateContext,
    with_transaction
);

define_api_function!(
    view_def_post_update,
    ClosureViewDefPostUpdateInterceptor<T, F>,
    ViewDefPostUpdateContext<T>
);

// VIEW PRE DELETE
define_interceptor!(
    context: ViewDefPreDeleteContext<T>,
    trait: ViewDefPreDeleteInterceptor,
    fields: {
        txn: &'a mut T,
        pre: &'a ViewDef,
    }
);

define_closure_interceptor!(
    ClosureViewDefPreDeleteInterceptor,
    ViewDefPreDeleteInterceptor,
    ViewDefPreDeleteContext,
    with_transaction
);

define_api_function!(
    view_def_pre_delete,
    ClosureViewDefPreDeleteInterceptor<T, F>,
    ViewDefPreDeleteContext<T>
);

use crate::impl_register_interceptor;

impl_register_interceptor!(
    ClosureViewDefPostCreateInterceptor<T, F>,
    ViewDefPostCreateContext<T>,
    ViewDefPostCreateInterceptor,
    view_def_post_create
);

impl_register_interceptor!(
    ClosureViewDefPreUpdateInterceptor<T, F>,
    ViewDefPreUpdateContext<T>,
    ViewDefPreUpdateInterceptor,
    view_def_pre_update
);

impl_register_interceptor!(
    ClosureViewDefPostUpdateInterceptor<T, F>,
    ViewDefPostUpdateContext<T>,
    ViewDefPostUpdateInterceptor,
    view_def_post_update
);

impl_register_interceptor!(
    ClosureViewDefPreDeleteInterceptor<T, F>,
    ViewDefPreDeleteContext<T>,
    ViewDefPreDeleteInterceptor,
    view_def_pre_delete
);