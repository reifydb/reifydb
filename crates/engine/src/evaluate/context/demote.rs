// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::evaluate::EvaluationContext;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::diagnostic::number::number_out_of_range;
use reifydb_core::value::number::SafeDemote;
use reifydb_core::{GetType, IntoSpan};

pub trait Demote {
    fn demote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
        To: GetType;
}

impl Demote for EvaluationContext {
    fn demote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
        To: GetType,
    {
        Demote::demote(&self, from, span)
    }
}

impl Demote for &EvaluationContext {
    fn demote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
        To: GetType,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => from
                .checked_demote()
                .ok_or_else(|| {
                    return crate::evaluate::Error(number_out_of_range(
                        span.into_span(),
                        To::get_type(),
                    ));
                })
                .map(Some),
            ColumnSaturationPolicy::Undefined => match from.checked_demote() {
                None => Ok(None),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::context::EvaluationColumn;
    use crate::evaluate::{Demote, EvaluationContext};
    use reifydb_catalog::column_policy::ColumnPolicyKind::Saturation;
    use reifydb_catalog::column_policy::ColumnSaturationPolicy::{Error, Undefined};
    use reifydb_core::Type;
    use reifydb_core::value::number::SafeDemote;
    use reifydb_core::{GetType, Span};

    #[test]
    fn test_demote_ok() {
        let mut ctx = EvaluationContext::testing();
        ctx.column =
            Some(EvaluationColumn { ty: Some(Type::Int1), policies: vec![Saturation(Error)] });

        let result = ctx.demote::<i16, i8>(1i16, || Span::testing_empty());
        assert_eq!(result, Ok(Some(1i8)));
    }

    #[test]
    fn test_demote_fail_with_error_policy() {
        let mut ctx = EvaluationContext::testing();
        ctx.column =
            Some(EvaluationColumn { ty: Some(Type::Int1), policies: vec![Saturation(Error)] });

        let err =
            ctx.demote::<TestI16, TestI8>(TestI16 {}, || Span::testing_empty()).err().unwrap();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "NUMBER_002");
    }

    #[test]
    fn test_demote_fail_with_undefined_policy() {
        let mut ctx = EvaluationContext::testing();
        ctx.column =
            Some(EvaluationColumn { ty: Some(Type::Int1), policies: vec![Saturation(Undefined)] });

        let result = ctx.demote::<TestI16, TestI8>(TestI16 {}, || Span::testing_empty()).unwrap();
        assert!(result.is_none());
    }

    pub struct TestI16 {}

    pub struct TestI8 {}

    impl GetType for TestI8 {
        fn get_type() -> Type {
            Type::Int8
        }
    }

    impl SafeDemote<TestI8> for TestI16 {
        fn checked_demote(self) -> Option<TestI8> {
            None
        }

        fn saturating_demote(self) -> TestI8 {
            unreachable!()
        }

        fn wrapping_demote(self) -> TestI8 {
            unreachable!()
        }
    }
}
