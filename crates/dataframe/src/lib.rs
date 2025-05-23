// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use column::{Column, ColumnValues};
pub use error::Error;
pub use frame::DataFrame;
pub use reference::{RowRef, ValueRef};
pub use transform::Append;

pub mod aggregate;
mod column;
mod display;
mod error;
mod frame;
mod iterator;
mod reference;
mod transform;
mod view;

pub type Result<T> = std::result::Result<T, Error>;

// pub fn inner_join_indices(l: &DataFrame, lr: &DataFrame, on: &str) -> Vec<(usize, usize)> {
//     let mut lr_index: HashMap<Value, Vec<usize>> = HashMap::new();
//
//     let start = Instant::now();
//     for (i, row) in lr.iter().enumerate() {
//         if let Some(k) = row.get(on) {
//             lr_index.entry(k.as_value()).or_default().push(i);
//         }
//     }
//     println!("1 took {:?}", start.elapsed());
//
//     let mut joined = vec![];
//
//     let start = Instant::now();
//     for (li, lrow) in l.iter().enumerate() {
//         if let Some(lkey) = lrow.get(on) {
//             if let Some(matches) = lr_index.get(&lkey.as_value()) {
//                 for &ri in matches {
//                     joined.push((li, ri));
//                 }
//             }
//         }
//     }
//
//     println!("2 took {:?}", start.elapsed());
//
//     joined
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::{Column, ColumnValues, DataFrame, inner_join_indices};
//     use rand::rngs::StdRng;
//     use rand::{Rng, SeedableRng};
//     use std::time::Instant;
//
//     fn generate_large_dataframe() -> DataFrame {
//         const N: usize = 1_000_000;
//         let mut rng = StdRng::seed_from_u64(42); // deterministic for testing
//
//         let mut ids = Vec::with_capacity(N);
//         let mut id_valids = Vec::with_capacity(N);
//
//         let mut scores = Vec::with_capacity(N);
//         let mut score_valids = Vec::with_capacity(N);
//
//         let mut passed = Vec::with_capacity(N);
//         let mut passed_valids = Vec::with_capacity(N);
//
//         for i in 0..N {
//             let id = (i + 1) as i16;
//             let score = (id % 100) as f64 + rng.gen_range(0.0..1.0);
//             let pass = score > 50.0;
//
//             ids.push(id);
//             id_valids.push(true);
//
//             scores.push(score);
//             score_valids.push(true);
//
//             passed.push(pass);
//             passed_valids.push(true);
//         }
//
//         DataFrame::new(vec![
//             Column { name: "id".into(), data: ColumnValues::Int2(ids, id_valids) },
//             // Column { name: "score".into(), data: ColumnValues::Float(scores, score_valids) },
//             Column { name: "passed".into(), data: ColumnValues::Bool(passed, passed_valids) },
//         ])
//     }
//
//     #[test]
//     fn test() {
//         let df = generate_large_dataframe();
//
//         let start = Instant::now();
//
//         let mut result = df
//             .iter()
//             // .filter(|row| match row.values[1] {
//             //     ValueRef::Float(f) if *f > 60.0 => true,
//             //     _ => false,
//             // })
//             .map(|row| {
//                 // Project: (id, score)
//                 (row.values[0].as_value(), row.values[1].as_value())
//             })
//             .collect::<Vec<_>>();
//
//         println!("{:?}", start.elapsed());
//
//         for (id, score) in result.iter().take(10) {
//             println!("id = {:?}, score = {:?}", id, score);
//         }
//     }
//
//     #[test]
//     fn join() {
//         // Define left table
//         // let left = DataFrame::new(
//         //     vec![
//         //         Column { name: "id".into(), data: ColumnData::Int(vec![1, 2, 3], vec![true; 3]) },
//         //         Column {
//         //             name: "name".into(),
//         //             data: ColumnData::Text(
//         //                 vec!["Alice".into(), "Bob".into(), "Carol".into()],
//         //                 vec![true; 3],
//         //             ),
//         //         },
//         //     ],
//         //     vec!["row0".into(), "row1".into(), "row2".into()],
//         // );
//         let left = generate_large_dataframe();
//
//         // Define right table
//         let right = DataFrame::new(vec![
//             Column { name: "id".into(), data: ColumnValues::Int2(vec![2, 3, 4], vec![true; 3]) },
//             // Column {
//             //     name: "score".into(),
//             //     data: ColumnValues::Float(vec![90.0, 75.5, 60.0], vec![true; 3]),
//             // },
//         ]);
//
//         let start = Instant::now();
//         // Join on "id"
//         let joined_indices = inner_join_indices(&left, &right, "id");
//
//         println!("Join result:");
//         for (li, ri) in joined_indices.iter().take(10) {
//             let lrow = left.iter().nth(*li).unwrap();
//             let rrow = right.iter().nth(*ri).unwrap();
//
//             let id = lrow.get("id").unwrap();
//             let name = lrow.get("passed").unwrap();
//             // let score = rrow.get("score").unwrap();
//
//             // println!("id: {:?}, name: {:?}, score: {:?}", id, name, score);
//         }
//
//         println!("took {:?}", start.elapsed());
//     }
// }
