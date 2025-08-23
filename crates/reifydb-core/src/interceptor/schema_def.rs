// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use crate::{
    define_api_function, define_closure_interceptor, define_interceptor,
    interface::SchemaDef,
};

// SCHEMA POST CREATE
define_interceptor!(
    context: SchemaDefPostCreateContext<T>,
    trait: SchemaDefPostCreateInterceptor,
    fields: {
        txn: &'a mut T,
        post: &'a SchemaDef,
    }
);

define_closure_interceptor!(
    ClosureSchemaDefPostCreateInterceptor,
    SchemaDefPostCreateInterceptor,
    SchemaDefPostCreateContext,
    with_transaction
);

define_api_function!(
    schema_def_post_create,
    ClosureSchemaDefPostCreateInterceptor<T, F>,
    SchemaDefPostCreateContext<T>
);

// SCHEMA PRE UPDATE
define_interceptor!(
    context: SchemaDefPreUpdateContext<T>,
    trait: SchemaDefPreUpdateInterceptor,
    fields: {
        txn: &'a mut T,
        pre: &'a SchemaDef,
    }
);

define_closure_interceptor!(
    ClosureSchemaDefPreUpdateInterceptor,
    SchemaDefPreUpdateInterceptor,
    SchemaDefPreUpdateContext,
    with_transaction
);

define_api_function!(
    schema_def_pre_update,
    ClosureSchemaDefPreUpdateInterceptor<T, F>,
    SchemaDefPreUpdateContext<T>
);

// SCHEMA POST UPDATE
define_interceptor!(
    context: SchemaDefPostUpdateContext<T>,
    trait: SchemaDefPostUpdateInterceptor,
    fields: {
        txn: &'a mut T,
        pre: &'a SchemaDef,
        post: &'a SchemaDef,
    }
);

define_closure_interceptor!(
    ClosureSchemaDefPostUpdateInterceptor,
    SchemaDefPostUpdateInterceptor,
    SchemaDefPostUpdateContext,
    with_transaction
);

define_api_function!(
    schema_def_post_update,
    ClosureSchemaDefPostUpdateInterceptor<T, F>,
    SchemaDefPostUpdateContext<T>
);

// SCHEMA PRE DELETE
define_interceptor!(
    context: SchemaDefPreDeleteContext<T>,
    trait: SchemaDefPreDeleteInterceptor,
    fields: {
        txn: &'a mut T,
        pre: &'a SchemaDef,
    }
);

define_closure_interceptor!(
    ClosureSchemaDefPreDeleteInterceptor,
    SchemaDefPreDeleteInterceptor,
    SchemaDefPreDeleteContext,
    with_transaction
);

define_api_function!(
    schema_def_pre_delete,
    ClosureSchemaDefPreDeleteInterceptor<T, F>,
    SchemaDefPreDeleteContext<T>
);

use crate::impl_register_interceptor;

impl_register_interceptor!(
    ClosureSchemaDefPostCreateInterceptor<T, F>,
    SchemaDefPostCreateContext<T>,
    SchemaDefPostCreateInterceptor,
    schema_def_post_create
);

impl_register_interceptor!(
    ClosureSchemaDefPreUpdateInterceptor<T, F>,
    SchemaDefPreUpdateContext<T>,
    SchemaDefPreUpdateInterceptor,
    schema_def_pre_update
);

impl_register_interceptor!(
    ClosureSchemaDefPostUpdateInterceptor<T, F>,
    SchemaDefPostUpdateContext<T>,
    SchemaDefPostUpdateInterceptor,
    schema_def_post_update
);

impl_register_interceptor!(
    ClosureSchemaDefPreDeleteInterceptor<T, F>,
    SchemaDefPreDeleteContext<T>,
    SchemaDefPreDeleteInterceptor,
    schema_def_pre_delete
);