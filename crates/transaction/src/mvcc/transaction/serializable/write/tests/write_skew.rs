// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later
// 
// // This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// // originally licensed under the Apache License, Version 2.0.
// // Original copyright:
// //   Copyright (c) 2024 Al Liu
// //
// // The original Apache License can be found at:
// //   http://www.apache.org/licenses/LICENSE-2.0
// 
// use super::*;
// 
// #[test]
// fn txn_write_skew() {
//   // accounts
//   let a999 = 999;
//   let a888 = 888;
//   let db: SerializableDb<u64, u64> = SerializableDb::new();
// 
//   // Set balance to $100 in each account.
//   let mut txn = db.write();
//   txn.insert(a999, 100).unwrap();
//   txn.insert(a888, 100).unwrap();
//   txn.commit().unwrap();
//   assert_eq!(1, db.version());
// 
//   let get_bal = |txn: &mut SerializableTransaction<u64, u64>, k: &u64| -> u64 {
//     let item = txn.get(k).unwrap().unwrap();
//     let val = *item.value();
//     val
//   };
// 
//   // Start two transactions, each would read both accounts and deduct from one account.
//   let mut txn1 = db.write();
// 
//   let mut sum = get_bal(&mut txn1, &a999);
//   sum += get_bal(&mut txn1, &a888);
//   assert_eq!(200, sum);
//   txn1.insert(a999, 0).unwrap(); // Deduct 100 from a999
// 
//   // Let's read this back.
//   let mut sum = get_bal(&mut txn1, &a999);
//   assert_eq!(0, sum);
//   sum += get_bal(&mut txn1, &a888);
//   assert_eq!(100, sum);
//   // Don't commit yet.
// 
//   let mut txn2 = db.write();
// 
//   let mut sum = get_bal(&mut txn2, &a999);
//   sum += get_bal(&mut txn2, &a888);
//   assert_eq!(200, sum);
//   txn2.insert(a888, 0).unwrap(); // Deduct 100 from a888
// 
//   // Let's read this back.
//   let mut sum = get_bal(&mut txn2, &a999);
//   assert_eq!(100, sum);
//   sum += get_bal(&mut txn2, &a888);
//   assert_eq!(100, sum);
// 
//   // Commit both now.
//   txn1.commit().unwrap();
//   txn2.commit().unwrap_err(); // This should fail
// 
//   assert_eq!(2, db.version());
// }
// 
// // https://wiki.postgresql.org/wiki/SSI#Black_and_White
// #[test]
// fn txn_write_skew_black_white() {
//   let db: SerializableDb<u64, &'static str> = SerializableDb::new();
// 
//   // Setup
//   let mut txn = db.write();
//   for i in 1..=10 {
//     if i % 2 == 1 {
//       txn.insert(i, "black").unwrap();
//     } else {
//       txn.insert(i, "white").unwrap();
//     }
//   }
//   txn.commit().unwrap();
// 
//   // txn1
//   let mut txn1 = db.write();
//   let indices = txn1
//     .iter()
//     .unwrap()
//     .filter_map(|e| {
//       if e.value() == "black" {
//         Some(*e.key())
//       } else {
//         None
//       }
//     })
//     .collect::<Vec<_>>();
//   for i in indices {
//     txn1.insert(i, "white").unwrap();
//   }
// 
//   // txn2
//   let mut txn2 = db.write();
//   let indices = txn2
//     .iter()
//     .unwrap()
//     .filter_map(|e| {
//       if e.value() == "white" {
//         Some(*e.key())
//       } else {
//         None
//       }
//     })
//     .collect::<Vec<_>>();
//   for i in indices {
//     txn2.insert(i, "black").unwrap();
//   }
//   txn2.commit().unwrap();
//   txn1.commit().unwrap_err();
// }
// 
// // https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
// #[test]
// fn txn_write_skew_intersecting_data() {
//   let db: SerializableDb<&'static str, u64> = SerializableDb::new();
// 
//   // Setup
//   let mut txn = db.write();
//   txn.insert("a1", 10).unwrap();
//   txn.insert("a2", 20).unwrap();
//   txn.insert("b1", 100).unwrap();
//   txn.insert("b2", 200).unwrap();
//   txn.commit().unwrap();
//   assert_eq!(1, db.version());
// 
//   let mut txn1 = db.write();
//   let val = txn1
//     .iter()
//     .unwrap()
//     .filter_map(|ele| {
//       if ele.key().starts_with('a') {
//         Some(*ele.value())
//       } else {
//         None
//       }
//     })
//     .sum::<u64>();
//   txn1.insert("b3", 30).unwrap();
//   assert_eq!(30, val);
// 
//   let mut txn2 = db.write();
//   let val = txn2
//     .iter()
//     .unwrap()
//     .filter_map(|ele| {
//       if ele.key().starts_with('b') {
//         Some(*ele.value())
//       } else {
//         None
//       }
//     })
//     .sum::<u64>();
//   txn2.insert("a3", 300).unwrap();
//   assert_eq!(300, val);
//   txn2.commit().unwrap();
//   txn1.commit().unwrap_err();
// 
//   let mut txn3 = db.write();
//   let val = txn3
//     .iter()
//     .unwrap()
//     .filter_map(|ele| {
//       if ele.key().starts_with('a') {
//         Some(*ele.value())
//       } else {
//         None
//       }
//     })
//     .sum::<u64>();
//   assert_eq!(330, val);
// }
// 
// // https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
// #[test]
// fn txn_write_skew_intersecting_data2() {
//   let db: SerializableDb<&'static str, u64> = SerializableDb::new();
// 
//   // Setup
//   let mut txn = db.write();
//   txn.insert("a1", 10).unwrap();
//   txn.insert("b1", 100).unwrap();
//   txn.insert("b2", 200).unwrap();
//   txn.commit().unwrap();
//   assert_eq!(1, db.version());
// 
//   //
//   let mut txn1 = db.write();
//   let val = txn1
//     .range("a".."b")
//     .unwrap()
//     .map(|ele| *ele.value())
//     .sum::<u64>();
//   txn1.insert("b3", 10).unwrap();
//   assert_eq!(10, val);
// 
//   let mut txn2 = db.write();
//   let val = txn2
//     .range("b".."c")
//     .unwrap()
//     .map(|ele| *ele.value())
//     .sum::<u64>();
//   txn2.insert("a3", 300).unwrap();
//   assert_eq!(300, val);
//   txn2.commit().unwrap();
//   txn1.commit().unwrap_err();
// 
//   let mut txn3 = db.write();
//   let val = txn3
//     .iter()
//     .unwrap()
//     .filter_map(|ele| {
//       if ele.key().starts_with('a') {
//         Some(*ele.value())
//       } else {
//         None
//       }
//     })
//     .sum::<u64>();
//   assert_eq!(310, val);
// }
// 
// // https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
// #[test]
// fn txn_write_skew_intersecting_data3() {
//   let db: SerializableDb<&'static str, u64> = SerializableDb::new();
// 
//   // Setup
//   let mut txn = db.write();
//   txn.insert("b1", 100).unwrap();
//   txn.insert("b2", 200).unwrap();
//   txn.commit().unwrap();
//   assert_eq!(1, db.version());
// 
//   let mut txn1 = db.write();
//   let val = txn1
//     .range("a".."b")
//     .unwrap()
//     .map(|ele| *ele.value())
//     .sum::<u64>();
//   txn1.insert("b3", 0).unwrap();
//   assert_eq!(0, val);
// 
//   let mut txn2 = db.write();
//   let val = txn2
//     .range("b".."c")
//     .unwrap()
//     .map(|ele| *ele.value())
//     .sum::<u64>();
//   txn2.insert("a3", 300).unwrap();
//   assert_eq!(300, val);
//   txn2.commit().unwrap();
//   txn1.commit().unwrap_err();
// 
//   let mut txn3 = db.write();
//   let val = txn3
//     .iter()
//     .unwrap()
//     .filter_map(|ele| {
//       if ele.key().starts_with('a') {
//         Some(*ele.value())
//       } else {
//         None
//       }
//     })
//     .sum::<u64>();
//   assert_eq!(300, val);
// }
// 
// // https://wiki.postgresql.org/wiki/SSI#Overdraft_Protection
// #[test]
// fn txn_write_skew_overdraft_protection() {
//   let db: SerializableDb<&'static str, u64> = SerializableDb::new();
// 
//   // Setup
//   let mut txn = db.write();
//   txn.insert("kevin", 1000).unwrap();
//   txn.commit().unwrap();
// 
//   // txn1
//   let mut txn1 = db.write();
//   let money = *txn1.get(&"kevin").unwrap().unwrap().value();
//   txn1.insert("kevin", money - 100).unwrap();
// 
//   // txn2
//   let mut txn2 = db.write();
//   let money = *txn2.get(&"kevin").unwrap().unwrap().value();
//   txn2.insert("kevin", money - 100).unwrap();
// 
//   txn1.commit().unwrap();
//   txn2.commit().unwrap_err();
// }
// 
// // https://wiki.postgresql.org/wiki/SSI#Primary_Colors
// #[test]
// fn txn_write_skew_primary_colors() {
//   let db: SerializableDb<u64, &'static str> = SerializableDb::new();
// 
//   // Setup
//   let mut txn = db.write();
//   for i in 1..=9000 {
//     if i % 3 == 1 {
//       txn.insert(i, "red").unwrap();
//     } else if i % 3 == 2 {
//       txn.insert(i, "yellow").unwrap();
//     } else {
//       txn.insert(i, "blue").unwrap();
//     }
//   }
//   txn.commit().unwrap();
// 
//   // txn1
//   let mut txn1 = db.write();
//   let indices = txn1
//     .iter()
//     .unwrap()
//     .filter_map(|e| {
//       if e.value() == "yellow" {
//         Some(*e.key())
//       } else {
//         None
//       }
//     })
//     .collect::<Vec<_>>();
//   for i in indices {
//     txn1.insert(i, "red").unwrap();
//   }
// 
//   // txn2
//   let mut txn2 = db.write();
//   let indices = txn2
//     .iter()
//     .unwrap()
//     .filter_map(|e| {
//       if e.value() == "blue" {
//         Some(*e.key())
//       } else {
//         None
//       }
//     })
//     .collect::<Vec<_>>();
//   for i in indices {
//     txn2.insert(i, "yellow").unwrap();
//   }
// 
//   // txn3
//   let mut txn3 = db.write();
//   let indices = txn3
//     .iter()
//     .unwrap()
//     .filter_map(|e| {
//       if e.value() == "blue" {
//         Some(*e.key())
//       } else {
//         None
//       }
//     })
//     .collect::<Vec<_>>();
//   for i in indices {
//     txn3.insert(i, "red").unwrap();
//   }
// 
//   txn1.commit().unwrap();
//   txn3.commit().unwrap_err();
//   txn2.commit().unwrap_err();
// }
