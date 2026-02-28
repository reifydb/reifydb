#![no_main]

use bumpalo::Bump;
use libfuzzer_sys::fuzz_target;
use std::fmt::Write;

#[path = "rql_gen.rs"]
mod rql_gen;

fuzz_target!(|input: rql_gen::RqlInput| {
    let mut w = rql_gen::LimitedWriter::new(10_000);
    if write!(w, "{input}").is_err() {
        return;
    }
    let s = w.buf;
    let bump = Bump::new();
    let _ = reifydb_rql::ast::parse_str(&bump, &s);
});
