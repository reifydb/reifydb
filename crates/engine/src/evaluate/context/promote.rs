// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::evaluate::EvaluationContext;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::diagnostic::number::number_out_of_range;
use reifydb_core::value::number::SafePromote;
use reifydb_core::{GetType, IntoOwnedSpan};

pub trait Promote {
    fn promote<From, To>(
        &self,
        from: From,
        span: impl IntoOwnedSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
        To: GetType;
}

impl Promote for EvaluationContext {
    fn promote<From, To>(
        &self,
        from: From,
        span: impl IntoOwnedSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
        To: GetType,
    {
        Promote::promote(&self, from, span)
    }
}

impl Promote for &EvaluationContext {
    fn promote<From, To>(
        &self,
        from: From,
        span: impl IntoOwnedSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
        To: GetType,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => from
                .checked_promote()
                .ok_or_else(|| {
                    return crate::evaluate::Error(number_out_of_range(
                        span.into_span(),
                        To::get_type(),
                    ));
                })
                .map(Some),
            ColumnSaturationPolicy::Undefined => match from.checked_promote() {
                None => Ok(None),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::context::EvaluationColumn;
    use crate::evaluate::context::promote::GetType;
    use crate::evaluate::{EvaluationContext, Promote};
    use reifydb_catalog::column_policy::ColumnPolicyKind::Saturation;
    use reifydb_catalog::column_policy::ColumnSaturationPolicy::{Error, Undefined};
    use reifydb_core::OwnedSpan;
    use reifydb_core::Type;
    use reifydb_core::value::number::SafePromote;

    #[test]
    fn test_promote_ok() {
        let mut ctx = EvaluationContext::testing();
        ctx.column =
            Some(EvaluationColumn { ty: Some(Type::Int2), policies: vec![Saturation(Error)] });

        let result = ctx.promote::<i8, i16>(1i8, || OwnedSpan::testing_empty());
        assert_eq!(result, Ok(Some(1i16)));
    }

    #[test]
    fn test_promote_fail_with_error_policy() {
        let mut ctx = EvaluationContext::testing();
        ctx.column =
            Some(EvaluationColumn { ty: Some(Type::Int2), policies: vec![Saturation(Error)] });

        let err =
            ctx.promote::<TestI8, TestI16>(TestI8 {}, || OwnedSpan::testing_empty()).err().unwrap();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "NUMBER_002")
    }

    #[test]
    fn test_promote_fail_with_undefined_policy() {
        let mut ctx = EvaluationContext::testing();
        ctx.column =
            Some(EvaluationColumn { ty: Some(Type::Int2), policies: vec![Saturation(Undefined)] });

        let result = ctx.promote::<TestI8, TestI16>(TestI8 {}, || OwnedSpan::testing_empty()).unwrap();
        assert!(result.is_none());
    }

    pub struct TestI8 {}

    impl SafePromote<TestI16> for TestI8 {
        fn checked_promote(self) -> Option<TestI16> {
            None
        }

        fn saturating_promote(self) -> TestI16 {
            unreachable!()
        }

        fn wrapping_promote(self) -> TestI16 {
            unreachable!()
        }
    }

    pub struct TestI16 {}

    impl GetType for TestI16 {
        fn get_type() -> Type {
            Type::Int16
        }
    }
}
