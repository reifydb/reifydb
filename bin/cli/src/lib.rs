// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

// use reifydb::Error;
//
// mod explain;
//
// pub fn cli(args: Vec<String>) -> Result<(), Error> {
//     if args.len() < 3 {
//         // return Err("Usage: reifydb explain [--ast|--logical|--physical]
// <query>".into());         panic!()
//     }
//
//     let command = &args[1];
//
//     if command != "explain" {
//         // return Err("Only 'explain' command is supported".into());
//         panic!()
//     }
//
//     let flag = &args[2];
//     let query = &args[3];
//
//     match flag.as_str() {
//         "--token" => explain::token(query),
//         "--ast" => explain::ast(query),
//         "--logical" => explain::logical_plan(query),
//         "--physical" => explain::physical_plan(query),
//         _ => panic!() // Err("Invalid flag. Use --ast, --logical, or --physical".into()),
//     }
// }
