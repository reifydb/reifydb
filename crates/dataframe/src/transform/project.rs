// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, DataFrame};

impl DataFrame {
    pub fn project<F>(&mut self, f: F) -> crate::Result<()>
    where
        F: FnOnce(&[&Column], usize) -> crate::Result<Vec<Column>>,
    {
        let row_count = self.columns.first().map_or(0, |col| col.data.len());

        let columns: Vec<&Column> = self.columns.iter().map(|c| c).collect();
        self.columns = f(&columns, row_count)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}

//     }
//
//     #[test]
//     fn test_project_text() {
//         let mut test_instance = DataFrame::new(vec![]);
//
//         test_instance
//             .project(vec![alias(
//                 Expression::Constant(Value::Text("some text".to_string())),
//                 Some("value"),
//             )])
//             .unwrap();
//
//         assert_eq!(test_instance.shape(), (1, 1));
//         assert_eq!(test_instance.columns[0].name, "value");
//
//         match &test_instance.columns[0].data {
//             ColumnValues::Text(vals, valid) => {
//                 assert_eq!(vals, &["some text"]);
//                 assert_eq!(valid, &[true]);
//             }
//             _ => panic!("Expected Text column"),
//         }
//     }
//
//     #[test]
//     fn test_project_bool() {
//         let mut test_instance = DataFrame::new(vec![]);
//
//         test_instance
//             .project(vec![alias(Expression::Constant(Value::Bool(true)), Some("value"))])
//             .unwrap();
//
//         assert_eq!(test_instance.shape(), (1, 1));
//         assert_eq!(test_instance.columns[0].name, "value");
//
//         match &test_instance.columns[0].data {
//             ColumnValues::Bool(vals, valid) => {
//                 assert_eq!(vals, &[true]);
//                 assert_eq!(valid, &[true]);
//             }
//             _ => panic!("Expected Bool column"),
//         }
//     }
//
//     #[test]
//     fn test_project_not_existing_column() {
//         let mut test_instance = DataFrame::new(vec![]);
//
//         let err = test_instance
//             .project(vec![alias(Expression::Column("missing".into()), None)])
//             .err()
//             .unwrap();
//         assert_eq!(err.to_string(), "unknown column 'missing'");
//     }
//
//     #[test]
//     fn test_project_handles_undefined() {
//         let mut test_instance = DataFrame::new(vec![col_int2("x", &[1, 2], &[true, false])]);
//
//         test_instance.project(vec![alias(Expression::Column("x".into()), None)]).unwrap();
//
//         match &test_instance.columns[0].data {
//             ColumnValues::Int2(vals, valid) => {
//                 assert_eq!(vals.len(), 2);
//                 assert_eq!(valid, &[true, false]);
//             }
//             _ => panic!("Expected Int2 column"),
//         }
//     }
//
//     #[test]
//     fn test_project_uses_expression_as_fallback_name() {
//         let mut test_instance = DataFrame::new(vec![col_bool("flag", &[true], &[true])]);
//
//         test_instance.project(vec![alias(Expression::Column("flag".into()), None)]).unwrap();
//         assert_eq!(test_instance.columns[0].name, "flag");
//     }
//
//     fn col_int2(name: &str, vals: &[i16], valid: &[bool]) -> Column {
//         Column { name: name.into(), data: ColumnValues::Int2(vals.to_vec(), valid.to_vec()) }
//     }
//
//     fn col_text(name: &str, vals: &[&str], valid: &[bool]) -> Column {
//         Column {
//             name: name.into(),
//             data: ColumnValues::Text(vals.iter().map(|s| s.to_string()).collect(), valid.to_vec()),
//         }
//     }
//
//     fn col_bool(name: &str, vals: &[bool], valid: &[bool]) -> Column {
//         Column { name: name.into(), data: ColumnValues::Bool(vals.to_vec(), valid.to_vec()) }
//     }
//
//     fn alias(expr: Expression, alias: Option<&str>) -> AliasExpression {
//         AliasExpression { expression: expr, alias: alias.map(|s| s.to_string()) }
//     }
// }
