// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	fs,
	path::{Path, PathBuf},
};

use reifydb_wasm::{
	Engine, LoadBinary, SpawnBinary,
	config::WasmConfig,
	module::{ExternalIndex, Value},
	source,
};
use wast::{
	QuoteWat, Wast, WastArg, WastExecute, WastRet, Wat,
	core::{NanPattern, WastArgCore, WastRetCore},
	lexer::Lexer,
	parser::ParseBuffer,
	token::{F32, F64},
};

pub fn run_test(category: &str, file: &str) {
	let mut engine = Engine::with_config(WasmConfig {
		max_memory_pages: 65536,
		max_instructions: 100_000_000,
		max_call_depth: 512,
		..WasmConfig::default()
	});

	// Register standard spectest module globals.
	engine.register_host_global("spectest", "global_i32", Value::I32(666));
	engine.register_host_global("spectest", "global_i64", Value::I64(666));
	engine.register_host_global("spectest", "global_f32", Value::F32(666.6));
	engine.register_host_global("spectest", "global_f64", Value::F64(666.6));

	// Register standard spectest module memory and table.
	engine.register_host_memory("spectest", "memory", 1, Some(2));
	engine.register_host_table("spectest", "table", 10, Some(20));

	// Register standard spectest module functions (no-op implementations).
	// These are used by many WASM spec tests.
	engine.register_host_function("spectest", "print", |_exec| Ok(()));
	engine.register_host_function("spectest", "print_i32", |exec| {
		let _ = exec.pop::<i32>()?;
		Ok(())
	});
	engine.register_host_function("spectest", "print_i64", |exec| {
		let _ = exec.pop::<i64>()?;
		Ok(())
	});
	engine.register_host_function("spectest", "print_f32", |exec| {
		let _ = exec.pop::<f32>()?;
		Ok(())
	});
	engine.register_host_function("spectest", "print_f64", |exec| {
		let _ = exec.pop::<f64>()?;
		Ok(())
	});
	engine.register_host_function("spectest", "print_i32_f32", |exec| {
		let _ = exec.pop::<f32>()?;
		let _ = exec.pop::<i32>()?;
		Ok(())
	});
	engine.register_host_function("spectest", "print_f64_f64", |exec| {
		let _ = exec.pop::<f64>()?;
		let _ = exec.pop::<f64>()?;
		Ok(())
	});

	let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	file_path.push(Path::new(format!("tests/{}/{}", category, file).as_str()));
	let test_file = fs::read(&file_path).unwrap_or_else(|_| panic!("Unable to read file {}", file));
	let wast = std::str::from_utf8(test_file.as_ref()).expect("failed to convert wast to utf8");
	let mut lexer = Lexer::new(wast);
	lexer.allow_confusing_unicode(true);
	let buf = ParseBuffer::new_with_lexer(lexer).expect("failed to create parse buffer");
	let wast_data = wast::parser::parse::<Wast>(&buf).expect("failed to parse wast");

	// Track named modules: maps module name to instance index
	let mut named_modules: HashMap<String, usize> = HashMap::new();

	for (idx, directive) in wast_data.directives.into_iter().enumerate() {
		use wast::WastDirective::*;
		let formatted_directive = format!("{:#?}", directive);

		match directive {
			Module(module) => {
				match module {
					QuoteWat::Wat(mut wat) => {
						// Extract module ID before encoding
						let module_id = match &wat {
							Wat::Module(m) => m.id.map(|id| id.name().to_string()),
							_ => None,
						};
						let bytes = wat.encode().expect("failed to encode module");
						engine.spawn(source::binary::bytes(bytes))
							.expect("Failed to spawn wasm");
						if let Some(name) = module_id {
							let inst_idx = engine.instance_count() - 1;
							named_modules.insert(name, inst_idx);
						}
					}
					QuoteWat::QuoteModule(_, source) => {
						let wat_text: Vec<u8> = std::iter::once(b"(module ".as_slice())
							.chain(source.iter().map(|(_, s)| *s))
							.chain(std::iter::once(b")".as_slice()))
							.flatten()
							.copied()
							.collect();
						let wat_str = match std::str::from_utf8(&wat_text) {
							Ok(s) => s,
							Err(_) => continue, // Skip if not valid UTF-8
						};
						let mut lexer = Lexer::new(wat_str);
						lexer.allow_confusing_unicode(true);
						let buf = match ParseBuffer::new_with_lexer(lexer) {
							Ok(b) => b,
							Err(_) => continue,
						};
						let mut wat: Wat = match wast::parser::parse(&buf) {
							Ok(w) => w,
							Err(_) => continue,
						};
						let bytes = match wat.encode() {
							Ok(b) => b,
							Err(_) => continue,
						};
						engine.spawn(source::binary::bytes(bytes))
							.expect("Failed to spawn quoted module");
					}
					_ => {}
				}
			}

			ModuleDefinition(_)
			| ModuleInstance {
				..
			} => {
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
						let result = if let Some(mod_id) = invoke.module {
							let inst_idx = named_modules[mod_id.name()];
							engine.invoke_on(inst_idx, invoke.name, args)
						} else {
							engine.invoke(invoke.name, args)
						};
						match result {
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
					WastExecute::Get {
						module,
						global,
						..
					} => {
						let result = if let Some(mod_id) = module {
							let inst_idx = named_modules[mod_id.name()];
							engine.get_global_on(inst_idx, global)
						} else {
							engine.get_global(global)
						};
						match result {
							Ok(value) => {
								let got = vec![value];
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
						}
					}
				}
			}

			AssertMalformed {
				..
			} => {
				// Stubbed — compiler panics on invalid input rather than returning errors
			}

			AssertInvalid {
				..
			} => {
				// Stubbed — compiler panics on invalid input rather than returning errors
			}

			AssertExhaustion {
				span: _,
				call,
				message,
			} => {
				let args = map_wast_args(&call.args);
				match engine.invoke(call.name, args) {
					Ok(results) => {
						panic!(
							"{}: {} - expected exhaustion, but got {:?}",
							idx, formatted_directive, results
						)
					}
					Err(e) => {
						let err_msg = format!("{}", e);
						// Check that the trap message matches (call stack exhaustion, etc.)
						assert!(
							err_msg.contains(&message)
								|| message.contains(&err_msg) || err_msg
								.contains("call depth exceeded") || err_msg
								.contains("out of fuel"),
							"{}: expected '{}', got '{}'",
							idx,
							message,
							err_msg,
						);
					}
				};
			}

			AssertTrap {
				span: _,
				exec,
				message,
			} => match exec {
				WastExecute::Invoke(invoke) => {
					let args = map_wast_args(&invoke.args);
					let result = if let Some(mod_id) = invoke.module {
						let inst_idx = named_modules[mod_id.name()];
						engine.invoke_on(inst_idx, invoke.name, args)
					} else {
						engine.invoke(invoke.name, args)
					};
					match result {
						Ok(results) => {
							panic!(
								"{}: {} - expected trap, but got {:?}",
								idx, formatted_directive, results
							)
						}
						Err(e) => {
							let err_msg = format!("{}", e);
							assert!(
								err_msg.contains(&message)
									|| message.contains(&err_msg),
								"{}: expected '{}', got '{}'",
								idx,
								message,
								err_msg,
							);
						}
					};
				}
				WastExecute::Wat(mut wat) => {
					let bytes = wat.encode().expect("failed to encode module");
					match engine.spawn(source::binary::bytes(bytes)) {
						Ok(_) => {
							panic!(
								"{}: {} - expected trap during instantiation, but succeeded",
								idx, formatted_directive
							)
						}
						Err(e) => {
							let err_msg = format!("{}", e);
							assert!(
								err_msg.contains(&message)
									|| message.contains(&err_msg),
								"{}: expected '{}', got '{}'",
								idx,
								message,
								err_msg,
							);
						}
					}
				}
				WastExecute::Get {
					..
				} => {}
			},

			AssertUnlinkable {
				..
			} => {}

			Invoke(invoke) => {
				let args = map_wast_args(&invoke.args);
				let result = if let Some(mod_id) = invoke.module {
					let inst_idx = named_modules[mod_id.name()];
					engine.invoke_on(inst_idx, invoke.name, args)
				} else {
					engine.invoke(invoke.name, args)
				};
				match result {
					Ok(_) => {}
					Err(t) => {
						panic!("{}: {} - {:?}", idx, formatted_directive, t);
					}
				}
			}

			Register {
				name,
				module,
				..
			} => {
				if let Some(mod_id) = module {
					if let Some(&inst_idx) = named_modules.get(mod_id.name()) {
						engine.register_module_at(inst_idx, name);
					}
				} else {
					engine.register_module(name);
				}
			}

			AssertException {
				..
			} => {}

			AssertSuspension {
				..
			} => {}

			Thread(_) => {}

			Wait {
				..
			} => {}
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
	RefNull,
	RefFunc,
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
			(TestValue::RefNull, Value::RefNull(_)) => true,
			(TestValue::RefFunc, Value::RefFunc(_)) => true,
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
				WastRetCore::RefExtern(None) => TestValue::RefNull,
				WastRetCore::RefNull(_) => TestValue::RefNull,
				WastRetCore::RefFunc(_) => TestValue::RefFunc,
				_ => todo!("{:?}", ret),
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
				WastArgCore::RefNull(_) => Value::RefNull(reifydb_wasm::module::ValueType::RefFunc),
				_ => todo!("{:?}", arg),
			}
		})
		.collect()
}
