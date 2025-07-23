// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;

use crate::evaluate::{Convert, Demote, Promote};
use crate::frame::ColumnValues;
use reifydb_core::error::diagnostic::cast;
use reifydb_core::{OwnedSpan, Type, err};

impl ColumnValues {
    pub fn cast(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> OwnedSpan,
    ) -> crate::Result<ColumnValues> {
        if let ColumnValues::Undefined(rows) = self {
            let mut result = ColumnValues::with_capacity(target, *rows);
            for _ in 0..*rows {
                result.push_undefined()
            }
            return Ok(result);
        }

        match target {
            _ if target == self.get_type() => Ok(self.clone()),
            _ if target.is_number() => self.to_number(target, ctx, span),
            _ if target.is_bool() => self.to_boolean(span),
            _ if target.is_utf8() => self.to_text(span),
            _ if target.is_temporal() => self.to_temporal(target, span),
            _ => {
                let source_type = self.get_type();
                err!(cast::unsupported_cast(span(), source_type, target))
            }
        }
    }
}
