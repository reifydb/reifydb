// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fs;
use std::path::{Path, PathBuf};

use wast::core::{NanPattern, WastArgCore, WastRetCore};
use wast::lexer::Lexer;
use wast::parser::ParseBuffer;
use wast::token::{F32, F64};
use wast::{QuoteWat, Wast, WastArg, WastExecute, WastRet};

use reifydb_wasm::module::{ExternalIndex, Value};
use reifydb_wasm::{source, Engine, SpawnBinary};

pub fn run_test(category: &str, file: &str) {
    let mut engine = Engine::default();

    let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    file_path.push(Path::new(format!("tests/{}/{}", category, file).as_str()));
    let test_file = fs::read(&file_path).unwrap_or_else(|_| panic!("Unable to read file {}", file));
    let wast = std::str::from_utf8(test_file.as_ref()).expect("failed to convert wast to utf8");
    let mut lexer = Lexer::new(wast);
    lexer.allow_confusing_unicode(true);
    let buf = ParseBuffer::new_with_lexer(lexer).expect("failed to create parse buffer");
    let wast_data = wast::parser::parse::<Wast>(&buf).expect("failed to parse wast");

    for (idx, directive) in wast_data.directives.into_iter().enumerate() {
        use wast::WastDirective::*;
        let formatted_directive = format!("{:#?}", directive);

        match directive {
            Module(module) => {
                match module {
                    QuoteWat::Wat(mut wat) => {
                        let bytes = wat.encode().expect("failed to encode module");
                        engine
                            .spawn(source::binary::bytes(bytes))
                            .expect("Failed to spawn wasm");
                    }
                    QuoteWat::QuoteModule(_, _) => {
                        // QuoteModule not supported yet, skip
                    }
                    _ => {}
                }
            }

            ModuleDefinition(_) | ModuleInstance { .. } => {
                // Not yet supported
            }

            AssertReturn {
                span: _,
                exec,
                results,
            } => {
                let expected = map_wast_to_test_value(&results);
                match exec {
                    WastExecute::Invoke(invoke) => {
                        let args = map_wast_args(&invoke.args);
                        match engine.invoke(invoke.name, args) {
                            Ok(results) => {
                                let got = results.to_vec();
                                assert_eq!(
                                    expected.len(),
                                    got.len(),
                                    "{}: {} - expected {} len, got {}",
                                    idx,
                                    formatted_directive,
                                    expected.len(),
                                    got.len()
                                );
                                expected.iter().zip(got).for_each(|(e, g)| {
                                    if !e.matches(&g) {
                                        panic!(
                                            "{}: {} - expected {:?}, got {:?}",
                                            idx, formatted_directive, e, g
                                        )
                                    }
                                });
                            }
                            Err(e) => {
                                panic!("{}: {} - {:?}", idx, formatted_directive, e);
                            }
                        };
                    }
                    WastExecute::Wat(_) => {}
                    WastExecute::Get { .. } => {}
                }
            }

            AssertMalformed { .. } => {
                // Stubbed — validation not yet implemented
            }

            AssertInvalid { .. } => {
                // Stubbed — validation not yet implemented
            }

            AssertExhaustion { .. } => {
                // Stubbed
            }

            AssertTrap {
                span: _,
                exec,
                message,
            } => {
                match exec {
                    WastExecute::Invoke(invoke) => {
                        let args = map_wast_args(&invoke.args);
                        match engine.invoke(invoke.name, args) {
                            Ok(results) => {
                                panic!(
                                    "{}: {} - expected trap, but got {:?}",
                                    idx, formatted_directive, results
                                )
                            }
                            Err(e) => {
                                assert_eq!(message, format!("{}", e));
                            }
                        };
                    }
                    WastExecute::Wat(_) => {}
                    WastExecute::Get { .. } => {}
                }
            }

            AssertUnlinkable { .. } => {}

            Invoke(invoke) => {
                let args = map_wast_args(&invoke.args);
                match engine.invoke(invoke.name, args) {
                    Ok(_) => {}
                    Err(t) => {
                        panic!("{}: {} - {:?}", idx, formatted_directive, t);
                    }
                }
            }

            Register { .. } => {}

            AssertException { .. } => {}

            AssertSuspension { .. } => {}

            Thread(_) => {}

            Wait { .. } => {}
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum TestValue {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    CanonicalNan,
    ArithmeticNan,
    RefExtern(u32),
}

impl TestValue {
    pub fn matches(self, value: &Value) -> bool {
        match (&self, value) {
            (TestValue::ArithmeticNan, Value::F32(r)) => r.is_nan(),
            (TestValue::CanonicalNan, Value::F32(r)) => r.is_nan(),
            (TestValue::ArithmeticNan, Value::F64(r)) => r.is_nan(),
            (TestValue::CanonicalNan, Value::F64(r)) => r.is_nan(),
            (TestValue::I32(l), Value::I32(r)) => l == r,
            (TestValue::I64(l), Value::I64(r)) => l == r,
            (TestValue::F32(l), Value::F32(r)) => l == r || l.to_bits() == r.to_bits(),
            (TestValue::F64(l), Value::F64(r)) => l == r || l.to_bits() == r.to_bits(),
            (TestValue::RefExtern(l), Value::RefExtern(r)) => l == &r.0,
            _ => false,
        }
    }
}

fn map_wast_to_test_value(args: &Vec<WastRet>) -> Vec<TestValue> {
    args.iter()
        .map(|ret| {
            let WastRet::Core(ret) = ret else {
                panic!("unsupported type");
            };
            match *ret {
                WastRetCore::I32(v) => TestValue::I32(v),
                WastRetCore::I64(v) => TestValue::I64(v),
                WastRetCore::F32(v) => from_nan_pattern_32(v),
                WastRetCore::F64(v) => from_nan_pattern_64(v),
                WastRetCore::RefExtern(Some(v)) => TestValue::RefExtern(v),
                _ => todo!(),
            }
        })
        .collect()
}

fn from_nan_pattern_32(arg: NanPattern<F32>) -> TestValue {
    use wast::core::NanPattern::*;
    match arg {
        Value(v) => TestValue::F32(f32::from_bits(v.bits)),
        CanonicalNan => TestValue::CanonicalNan,
        ArithmeticNan => TestValue::ArithmeticNan,
    }
}

fn from_nan_pattern_64(arg: NanPattern<F64>) -> TestValue {
    use wast::core::NanPattern::*;
    match arg {
        Value(v) => TestValue::F64(f64::from_bits(v.bits)),
        CanonicalNan => TestValue::CanonicalNan,
        ArithmeticNan => TestValue::ArithmeticNan,
    }
}

fn map_wast_args(args: &Vec<WastArg>) -> Vec<Value> {
    args.iter()
        .map(|ret| {
            let WastArg::Core(arg) = ret else {
                panic!("unsupported type");
            };
            match arg {
                WastArgCore::I32(v) => Value::I32(*v),
                WastArgCore::I64(v) => Value::I64(*v),
                WastArgCore::F32(v) => Value::F32(f32::from_bits(v.bits)),
                WastArgCore::F64(v) => Value::F64(f64::from_bits(v.bits)),
                WastArgCore::RefExtern(v) => Value::RefExtern(ExternalIndex(*v)),
                _ => todo!(),
            }
        })
        .collect()
}
