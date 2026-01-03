// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode instruction encoding and decoding.

use super::opcode::Opcode;

/// Bytecode reader for decoding instructions.
pub struct BytecodeReader<'a> {
	bytecode: &'a [u8],
	position: usize,
}

impl<'a> BytecodeReader<'a> {
	/// Create a new bytecode reader.
	pub fn new(bytecode: &'a [u8]) -> Self {
		Self {
			bytecode,
			position: 0,
		}
	}

	/// Get current position in the bytecode.
	pub fn position(&self) -> usize {
		self.position
	}

	/// Set the position in the bytecode.
	pub fn set_position(&mut self, pos: usize) {
		self.position = pos;
	}

	/// Check if we've reached the end of the bytecode.
	pub fn at_end(&self) -> bool {
		self.position >= self.bytecode.len()
	}

	/// Read an opcode from the bytecode.
	pub fn read_opcode(&mut self) -> Option<Opcode> {
		let byte = self.read_u8()?;
		Opcode::try_from(byte).ok()
	}

	/// Read a single byte.
	pub fn read_u8(&mut self) -> Option<u8> {
		if self.position < self.bytecode.len() {
			let byte = self.bytecode[self.position];
			self.position += 1;
			Some(byte)
		} else {
			None
		}
	}

	/// Read a 16-bit unsigned integer (little-endian).
	pub fn read_u16(&mut self) -> Option<u16> {
		if self.position + 2 <= self.bytecode.len() {
			let bytes = &self.bytecode[self.position..self.position + 2];
			self.position += 2;
			Some(u16::from_le_bytes([bytes[0], bytes[1]]))
		} else {
			None
		}
	}

	/// Read a 16-bit signed integer (little-endian).
	pub fn read_i16(&mut self) -> Option<i16> {
		self.read_u16().map(|v| v as i16)
	}

	/// Read a 32-bit unsigned integer (little-endian).
	pub fn read_u32(&mut self) -> Option<u32> {
		if self.position + 4 <= self.bytecode.len() {
			let bytes = &self.bytecode[self.position..self.position + 4];
			self.position += 4;
			Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
		} else {
			None
		}
	}

	/// Peek at the next byte without consuming it.
	pub fn peek_u8(&self) -> Option<u8> {
		if self.position < self.bytecode.len() {
			Some(self.bytecode[self.position])
		} else {
			None
		}
	}
}

/// Bytecode writer for encoding instructions.
pub struct BytecodeWriter {
	bytecode: Vec<u8>,
}

impl BytecodeWriter {
	/// Create a new bytecode writer.
	pub fn new() -> Self {
		Self {
			bytecode: Vec::new(),
		}
	}

	/// Get current position (length of bytecode so far).
	pub fn position(&self) -> usize {
		self.bytecode.len()
	}

	/// Emit an opcode.
	pub fn emit_opcode(&mut self, opcode: Opcode) {
		self.bytecode.push(opcode as u8);
	}

	/// Emit a single byte.
	pub fn emit_u8(&mut self, value: u8) {
		self.bytecode.push(value);
	}

	/// Emit a 16-bit unsigned integer (little-endian).
	pub fn emit_u16(&mut self, value: u16) {
		self.bytecode.extend_from_slice(&value.to_le_bytes());
	}

	/// Emit a 16-bit signed integer (little-endian).
	pub fn emit_i16(&mut self, value: i16) {
		self.emit_u16(value as u16);
	}

	/// Emit a 32-bit unsigned integer (little-endian).
	pub fn emit_u32(&mut self, value: u32) {
		self.bytecode.extend_from_slice(&value.to_le_bytes());
	}

	/// Append raw bytecode from another buffer.
	pub fn append(&mut self, bytes: &[u8]) {
		self.bytecode.extend_from_slice(bytes);
	}

	/// Patch a u16 at the given position.
	pub fn patch_u16(&mut self, position: usize, value: u16) {
		let bytes = value.to_le_bytes();
		self.bytecode[position] = bytes[0];
		self.bytecode[position + 1] = bytes[1];
	}

	/// Patch a relative jump offset.
	/// Call this after emitting the jump target code, passing the position
	/// where the offset placeholder was emitted.
	pub fn patch_jump(&mut self, jump_pos: usize) {
		let current = self.position();
		let offset = (current as i32 - jump_pos as i32 - 2) as i16;
		self.patch_u16(jump_pos, offset as u16);
	}

	/// Patch a jump to a specific target position.
	/// `jump_pos` is where the i16 offset was emitted, `target` is the destination.
	pub fn patch_jump_at(&mut self, jump_pos: usize, target: usize) {
		let offset = (target as i32 - jump_pos as i32 - 2) as i16;
		self.patch_u16(jump_pos, offset as u16);
	}

	/// Finish writing and return the bytecode.
	pub fn finish(self) -> Vec<u8> {
		self.bytecode
	}
}

impl Default for BytecodeWriter {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_write_and_read() {
		let mut writer = BytecodeWriter::new();
		writer.emit_opcode(Opcode::PushConst);
		writer.emit_u16(42);
		writer.emit_opcode(Opcode::Halt);

		let bytecode = writer.finish();
		let mut reader = BytecodeReader::new(&bytecode);

		assert_eq!(reader.read_opcode(), Some(Opcode::PushConst));
		assert_eq!(reader.read_u16(), Some(42));
		assert_eq!(reader.read_opcode(), Some(Opcode::Halt));
		assert!(reader.at_end());
	}

	#[test]
	fn test_u32_read_write() {
		let mut writer = BytecodeWriter::new();
		writer.emit_u32(0x12345678);
		writer.emit_u32(0xDEADBEEF);

		let bytecode = writer.finish();
		let mut reader = BytecodeReader::new(&bytecode);

		assert_eq!(reader.read_u32(), Some(0x12345678));
		assert_eq!(reader.read_u32(), Some(0xDEADBEEF));
		assert!(reader.at_end());
	}

	#[test]
	fn test_jump_patching() {
		let mut writer = BytecodeWriter::new();
		writer.emit_opcode(Opcode::Jump);
		let jump_pos = writer.position();
		writer.emit_u16(0); // Placeholder

		// Emit some code
		writer.emit_opcode(Opcode::Nop);
		writer.emit_opcode(Opcode::Nop);

		// Patch the jump to current position
		writer.patch_jump(jump_pos);

		writer.emit_opcode(Opcode::Halt);

		let bytecode = writer.finish();
		let mut reader = BytecodeReader::new(&bytecode);

		assert_eq!(reader.read_opcode(), Some(Opcode::Jump));
		let offset = reader.read_i16().unwrap();
		assert_eq!(offset, 2); // Jump over two Nop opcodes
	}
}
