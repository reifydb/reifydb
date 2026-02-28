#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bumpalo::Bump;
use libfuzzer_sys::fuzz_target;
use std::fmt::Write;

#[path = "rql_gen.rs"]
mod rql_gen;

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    let mut u = Unstructured::new(data);
    let input: rql_gen::RqlInput = match rql_gen::RqlInput::arbitrary(&mut u) {
        Ok(v) => v,
        Err(_) => return,
    };
    let mut w = rql_gen::LimitedWriter::new(10_000);
    if write!(w, "{input}").is_err() {
        return;
    }
    let s = w.buf;
    let bump = Bump::new();
    let _ = reifydb_rql::ast::parse_str(&bump, &s);
});
