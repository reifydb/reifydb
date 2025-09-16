// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
//
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
//     let query = args[3..].join(" ");
//
//     match flag.as_str() {
//         "--tokenize" => explain::tokenize(&query),
//         "--ast" => explain::ast(&query),
//         "--logical" => explain::logical_plan(&query),
//         "--physical" => explain::physical_plan(&query),
//         // _ => Err(format!("Unknown explain flag: {}", flag)),
//         _ => unimplemented!(),
//     }
// }
