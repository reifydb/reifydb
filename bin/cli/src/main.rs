// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::engine;
use reifydb::engine::Engine;
use reifydb::engine::execute::execute_plan;
use reifydb::rql::ast;
use reifydb::rql::plan::plan;

fn main() {
    let engine = engine::svl::Engine::new();

    let rx = engine.begin_read_only().unwrap();

    let statements = ast::parse(
        r#"
        from users
        limit 2
        select id, name
    "#,
    );

    for statement in statements {
        let plan = plan(statement).unwrap();

        let result = execute_plan(&plan, &rx).unwrap();
        for row in result {
            println!("{:?}", row);
        }
    }
}
