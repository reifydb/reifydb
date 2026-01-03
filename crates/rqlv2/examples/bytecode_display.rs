// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Example demonstrating the bytecode display module.

use reifydb_rqlv2::bytecode::{BytecodeWriter, CompiledProgram, Constant, Opcode, OperatorKind, SourceDef};

fn main() {
	// Create a simple program
	let mut program = CompiledProgram::new();

	// Add some constants
	program.add_constant(Constant::Undefined);
	program.add_constant(Constant::Int(42));
	program.add_constant(Constant::String("users".to_string()));
	program.add_constant(Constant::String("id".to_string()));
	program.add_constant(Constant::Float(3.14159));

	// Add a source (table scan)
	program.add_source(SourceDef {
		name: "users".to_string(),
		alias: None,
	});
	program.add_source(SourceDef {
		name: "orders".to_string(),
		alias: Some("o".to_string()),
	});

	// Add column lists
	program.add_column_list(vec!["id".to_string(), "name".to_string(), "email".to_string()]);
	program.add_column_list(vec!["*".to_string()]);

	// Build bytecode
	let mut writer = BytecodeWriter::new();

	// Source(0) - scan "users" table
	writer.emit_opcode(Opcode::Source);
	writer.emit_u16(0);

	// PushColList(0) - push column list
	writer.emit_opcode(Opcode::PushColList);
	writer.emit_u16(0);

	// Apply(Filter) - apply filter operator
	writer.emit_opcode(Opcode::Apply);
	writer.emit_u8(OperatorKind::Filter as u8);

	// Collect - collect to columns
	writer.emit_opcode(Opcode::Collect);

	// PushConst(1) - push Int(42)
	writer.emit_opcode(Opcode::PushConst);
	writer.emit_u16(1);

	// LoadVar(0x2A) - load variable
	writer.emit_opcode(Opcode::LoadVar);
	writer.emit_u32(0x0000002A);

	// Jump forward
	writer.emit_opcode(Opcode::Jump);
	let jump_pos = writer.position();
	writer.emit_i16(0); // Placeholder

	// Nop, Nop
	writer.emit_opcode(Opcode::Nop);
	writer.emit_opcode(Opcode::Nop);

	// Patch jump to here
	writer.patch_jump(jump_pos);

	// Collect
	writer.emit_opcode(Opcode::Collect);

	// Halt
	writer.emit_opcode(Opcode::Halt);

	program.bytecode = writer.finish();

	// Display the program
	println!("{}", program);
}
