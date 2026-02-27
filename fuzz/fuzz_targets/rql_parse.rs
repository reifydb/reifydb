#![no_main]

use bumpalo::Bump;
use libfuzzer_sys::fuzz_target;

#[path = "rql_gen.rs"]
mod rql_gen;

fuzz_target!(|input: rql_gen::RqlInput| {
    let s = input.to_string();
    if s.len() > 10_000 {
        return;
    }
    let bump = Bump::new();
    let _ = reifydb_rql::ast::parse_str(&bump, &s);
});
