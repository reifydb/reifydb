// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
