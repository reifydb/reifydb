// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::evaluate::{Context, EvaluationColumn, evaluate};
use crate::execute::Executor;
use reifydb_core::ValueKind;
use reifydb_core::num::{SafeDemote, SafePromote};
use reifydb_diagnostic::Span;
use reifydb_frame::ColumnValues;
use reifydb_rql::plan::InsertIntoTablePlan;
use reifydb_transaction::Tx;

impl Executor {
    pub(crate) fn insert_into_table(
        &mut self,
        tx: &mut impl Tx,
        plan: InsertIntoTablePlan,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            InsertIntoTablePlan::Values { schema, table, columns, rows_to_insert } => {
                let mut rows = Vec::with_capacity(rows_to_insert.len());

                for row in rows_to_insert {
                    let mut row_values = Vec::with_capacity(row.len());
                    for (idx, expr) in row.into_iter().enumerate() {
                        let column = &columns[idx];

                        let context = Context {
                            column: Some(EvaluationColumn {
                                name: column.name.clone(),
                                value: column.value,
                                policies: column.policies.clone(),
                            }),
                            frame: None,
                        };

                        // let span = expr.span().clone();
                        let lazy_span = expr.lazy_span();
                        match &expr {
                            expr => {
                                let cvs = evaluate(expr, &context, &[], 1)?;
                                match cvs.len() {
                                    1 => {
                                        // FIXME ensure its the right value
                                        // otherwise try to demote
                                        // otherwise saturate according to the policy
                                        // let r = match (column.value, &cvs) {
                                        //     (ValueKind::Int1, ColumnValues::Int1(_, _)) => cvs,
                                        //     (
                                        //         ValueKind::Int1,
                                        //         ColumnValues::Int2(values, validity),
                                        //     ) => {
                                        //         let slice = values.as_slice();
                                        //         let mut res = ColumnValues::with_capacity(
                                        //             ValueKind::Int1,
                                        //             slice.len(),
                                        //         );
                                        //
                                        //         for (i, &val) in slice.iter().enumerate() {
                                        //             if validity[i] {
                                        //                 match context.demote(val, &lazy_span)? {
                                        //                     Some(value) => {
                                        //                         res.push_i8(value);
                                        //                     }
                                        //                     None => res.push_undefined(),
                                        //                 }
                                        //             } else {
                                        //                 res.push_undefined()
                                        //             }
                                        //         }
                                        //
                                        //         res
                                        //     }
                                        //     (ValueKind::Int2, ColumnValues::Int2(_, _)) => cvs,
                                        //
                                        //     (v, cvs) => unimplemented!("{v:?} {cvs:?}"),
                                        // };

                                        let r = adjust_column(
                                            column.value,
                                            &cvs,
                                            &context,
                                            &lazy_span,
                                        )?;

                                        row_values.push(r.get(0).as_value());
                                    }
                                    _ => unimplemented!(),
                                }
                            }
                        }
                    }
                    rows.push(row_values);
                }

                let result = tx.insert_into_table(schema.as_str(), table.as_str(), rows).unwrap();
                Ok(ExecutionResult::InsertIntoTable { schema, table, inserted: result.inserted })
            }
        }
    }
}

fn adjust_column(
    target: ValueKind,
    source: &ColumnValues,
    context: &Context,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    use ValueKind::*;

    if target == source.kind() {
        return Ok(source.clone());
    }

    if let ColumnValues::Int2(values, validity) = source {
        if target == Int1 {
            return demote_vec::<i16, i8>(
                values,
                validity,
                context,
                &span,
                Int1,
                ColumnValues::push_i8,
            );
        }
    }

    if let ColumnValues::Int1(values, validity) = source {
        if target == Int2 {
            return promote_vec::<i8, i16>(
                values,
                validity,
                context,
                &span,
                Int2,
                ColumnValues::push_i16,
            );
        }
    }

    match source {
        ColumnValues::Int2(values, validity) if target == Int1 => {
            let mut out = ColumnValues::with_capacity(Int1, values.len());
            for (i, &val) in values.iter().enumerate() {
                if validity[i] {
                    match context.demote::<i16, i8>(val, &span)? {
                        Some(v) => out.push_i8(v),
                        None => out.push_undefined(),
                    }
                } else {
                    out.push_undefined();
                }
            }
            Ok(out)
        }

        ColumnValues::Int4(values, validity) if target == Int2 => {
            let mut out = ColumnValues::with_capacity(Int2, values.len());
            for (i, &val) in values.iter().enumerate() {
                if validity[i] {
                    match context.demote::<i32, i16>(val, &span)? {
                        Some(v) => out.push_i16(v),
                        None => out.push_undefined(),
                    }
                } else {
                    out.push_undefined();
                }
            }
            Ok(out)
        }

        // ColumnValues::Uint2(values, validity) if target_kind == Uint1 => {
        //     let mut out = ColumnValues::with_capacity(Uint1, values.len());
        //     for (i, &val) in values.iter().enumerate() {
        //         if validity[i] {
        //             match context.demote::<u16, u8>(val, &span)? {
        //                 Some(v) => out.push_u8(v),
        //                 None => out.push_undefined(),
        //             }
        //         } else {
        //             out.push_undefined();
        //         }
        //     }
        //     Ok(out)
        // }
        //
        // UInt4(values, validity) if target_kind == UInt2 => {
        //     let mut out = ColumnValues::with_capacity(UInt2, values.len());
        //     for (i, &val) in values.iter().enumerate() {
        //         if validity[i] {
        //             match context.demote::<u32, u16>(val, &span)? {
        //                 Some(v) => out.push_u16(v),
        //                 None => out.push_undefined(),
        //             }
        //         } else {
        //             out.push_undefined();
        //         }
        //     }
        //     Ok(out)
        // }

        // _ => Err(Error::UnsupportedConversion {
        //     from: source.kind(),
        //     to: target_kind,
        // }),
        _ => unimplemented!("{source:?} {target:?}"),
    }
}

fn demote_vec<From, To>(
    values: &[From],
    validity: &[bool],
    context: &Context,
    span: impl Fn() -> Span,
    target_kind: ValueKind,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeDemote<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (i, &val) in values.iter().enumerate() {
        if validity[i] {
            match context.demote::<From, To>(val, &span)? {
                Some(v) => push(&mut out, v),
                None => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn promote_vec<From, To>(
    values: &[From],
    validity: &[bool],
    context: &Context,
    span: impl Fn() -> Span,
    target_kind: ValueKind,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafePromote<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (i, &val) in values.iter().enumerate() {
        if validity[i] {
            match context.promote::<From, To>(val, &span)? {
                Some(v) => push(&mut out, v),
                None => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
