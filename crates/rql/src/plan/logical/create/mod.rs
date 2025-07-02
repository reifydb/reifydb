// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod deferred_view;
mod schema;
mod series;
mod table;

use crate::ast::{AstCreate, AstPolicy, AstPolicyKind};
use crate::plan::logical::{Compiler, LogicalPlan};
use reifydb_catalog::column_policy::ColumnPolicyKind::Saturation;
use reifydb_catalog::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};

impl Compiler {
    pub(crate) fn compile_create(ast: AstCreate) -> crate::Result<LogicalPlan> {
        match ast {
            AstCreate::DeferredView(node) => Self::compile_deferred_view(node),
            AstCreate::Schema(node) => Self::compile_create_schema(node),
            AstCreate::Series(node) => Self::compile_create_series(node),
            AstCreate::Table(node) => Self::compile_create_table(node),
        }
    }
}

pub(crate) fn convert_policy(ast: &AstPolicy) -> ColumnPolicyKind {
    use ColumnPolicyKind::*;

    match ast.policy {
        AstPolicyKind::Saturation => {
            if ast.value.is_literal_undefined() {
                return Saturation(ColumnSaturationPolicy::Undefined);
            }
            let ident = ast.value.as_identifier().value();
            match ident {
                "error" => Saturation(ColumnSaturationPolicy::Error),
                // "saturate" => Some(Saturation(Saturate)),
                // "wrap" => Some(Saturation(Wrap)),
                // "zero" => Some(Saturation(Zero)),
                _ => unimplemented!(),
            }
        }
        AstPolicyKind::Default => unimplemented!(),
        AstPolicyKind::NotUndefined => unimplemented!(),
    }
}
