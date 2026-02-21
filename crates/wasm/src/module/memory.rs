// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::module::{MemoryIndex, PAGE_SIZE, Trap, TrapOutOfRange};

pub type MemoryOffset = u32;
pub type MemoryFlags = u32;

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Memory {
	pub data: Vec<u8>,
	pub max: Option<u32>,
}

// ---------------------------------------------------------------------------
// MemoryReader trait + implementations
// ---------------------------------------------------------------------------

pub trait MemoryReader {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap>
	where
		Self: Sized;
}

impl MemoryReader for i8 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let val = memory.data[idx] as i8;
		Ok(val)
	}
}

impl MemoryReader for u8 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let val = memory.data[idx];
		Ok(val)
	}
}

impl MemoryReader for i16 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 1 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as i16;
		let _2 = memory.data[idx + 1] as i16;

		let res = _2 << 8 | _1;

		Ok(res)
	}
}

impl MemoryReader for u16 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 1 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as u16;
		let _2 = memory.data[idx + 1] as u16;

		let res = _2 << 8 | _1;

		Ok(res)
	}
}

impl MemoryReader for i32 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 3 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as i32;
		let _2 = memory.data[idx + 1] as i32;
		let _3 = memory.data[idx + 2] as i32;
		let _4 = memory.data[idx + 3] as i32;

		let res = _4 << 24 | _3 << 16 | _2 << 8 | _1;

		Ok(res)
	}
}

impl MemoryReader for u32 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 3 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as u32;
		let _2 = memory.data[idx + 1] as u32;
		let _3 = memory.data[idx + 2] as u32;
		let _4 = memory.data[idx + 3] as u32;

		let res = _4 << 24 | _3 << 16 | _2 << 8 | _1;

		Ok(res)
	}
}

impl MemoryReader for f32 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 3 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as u32;
		let _2 = memory.data[idx + 1] as u32;
		let _3 = memory.data[idx + 2] as u32;
		let _4 = memory.data[idx + 3] as u32;

		let res = _4 << 24 | _3 << 16 | _2 << 8 | _1;

		Ok(f32::from_bits(res))
	}
}

impl MemoryReader for i64 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 7 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as i64;
		let _2 = memory.data[idx + 1] as i64;
		let _3 = memory.data[idx + 2] as i64;
		let _4 = memory.data[idx + 3] as i64;
		let _5 = memory.data[idx + 4] as i64;
		let _6 = memory.data[idx + 5] as i64;
		let _7 = memory.data[idx + 6] as i64;
		let _8 = memory.data[idx + 7] as i64;

		let res = _8 << 56 | _7 << 48 | _6 << 40 | _5 << 32 | _4 << 24 | _3 << 16 | _2 << 8 | _1;

		Ok(res)
	}
}

impl MemoryReader for u64 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 7 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as u64;
		let _2 = memory.data[idx + 1] as u64;
		let _3 = memory.data[idx + 2] as u64;
		let _4 = memory.data[idx + 3] as u64;
		let _5 = memory.data[idx + 4] as u64;
		let _6 = memory.data[idx + 5] as u64;
		let _7 = memory.data[idx + 6] as u64;
		let _8 = memory.data[idx + 7] as u64;

		let res = _8 << 56 | _7 << 48 | _6 << 40 | _5 << 32 | _4 << 24 | _3 << 16 | _2 << 8 | _1;

		Ok(res)
	}
}

impl MemoryReader for f64 {
	fn read(memory: &Memory, idx: MemoryIndex) -> Result<Self, Trap> {
		if idx + 7 >= memory.len() {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
		}

		let _1 = memory.data[idx] as u64;
		let _2 = memory.data[idx + 1] as u64;
		let _3 = memory.data[idx + 2] as u64;
		let _4 = memory.data[idx + 3] as u64;
		let _5 = memory.data[idx + 4] as u64;
		let _6 = memory.data[idx + 5] as u64;
		let _7 = memory.data[idx + 6] as u64;
		let _8 = memory.data[idx + 7] as u64;

		let res = _8 << 56 | _7 << 48 | _6 << 40 | _5 << 32 | _4 << 24 | _3 << 16 | _2 << 8 | _1;

		Ok(f64::from_bits(res))
	}
}

// ---------------------------------------------------------------------------
// MemoryWriter trait + macro + implementations
// ---------------------------------------------------------------------------

pub trait MemoryWriter {
	fn write(memory: &mut Memory, idx: MemoryIndex, value: Self) -> Result<(), Trap>;
}

macro_rules! memory_writer {
	($type:ty) => {
		impl MemoryWriter for $type {
			fn write(memory: &mut Memory, idx: MemoryIndex, value: Self) -> Result<(), Trap> {
				let size = size_of::<$type>();
				if idx + size > memory.len() {
					return Err(Trap::OutOfRange(TrapOutOfRange::Memory(idx)));
				}
				memory.data[idx..idx + size].copy_from_slice(&value.to_le_bytes());
				Ok(())
			}
		}
	};
}

memory_writer!(i8);
memory_writer!(u8);
memory_writer!(i16);
memory_writer!(u16);
memory_writer!(i32);
memory_writer!(u32);
memory_writer!(i64);
memory_writer!(u64);
memory_writer!(f32);
memory_writer!(f64);

// ---------------------------------------------------------------------------
// Memory impl
// ---------------------------------------------------------------------------

impl Memory {
	pub fn read<R>(&self, idx: MemoryIndex) -> Result<R, Trap>
	where
		R: MemoryReader,
	{
		R::read(self, idx)
	}

	pub fn write<W>(&mut self, idx: MemoryIndex, value: W) -> Result<(), Trap>
	where
		W: MemoryWriter,
	{
		W::write(self, idx, value)
	}

	pub fn grow(&mut self, pages: u32) -> Result<u32, Trap> {
		let previous_pages = (self.data.len() / PAGE_SIZE as usize) as u32;
		let new_pages = previous_pages.saturating_add(pages);

		if let Some(max) = self.max {
			if new_pages > max {
				return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
			}
		}

		self.data.resize((new_pages as usize) * (PAGE_SIZE as usize), 0);

		Ok(previous_pages)
	}

	pub fn grow_checked(&mut self, pages: u32, config_max_pages: u32) -> Result<u32, Trap> {
		let previous_pages = (self.data.len() / PAGE_SIZE as usize) as u32;
		let new_pages = previous_pages.saturating_add(pages);

		let effective_max = match self.max {
			Some(module_max) => module_max.min(config_max_pages),
			None => config_max_pages,
		};

		if new_pages > effective_max {
			return Err(Trap::OutOfRange(TrapOutOfRange::Memory(0)));
		}

		self.data.resize((new_pages as usize) * (PAGE_SIZE as usize), 0);

		Ok(previous_pages)
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
	mod i8 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i8>(8).unwrap();
			assert_eq!(result, 9)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i8>(11);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(11))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<i8>(0, i8::MAX).unwrap();
			assert_eq!(ti.data, vec![127, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			ti.write::<i8>(5, i8::MIN).unwrap();
			assert_eq!(ti.data, vec![127, 0, 0, 0, 0, 128, 0, 0, 0, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<i8>(10, 42);
			assert!(result.is_err());

			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod u8 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u8>(8).unwrap();
			assert_eq!(result, 9)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u8>(11);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(11))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<u8>(0, 42).unwrap();
			assert_eq!(ti.data, vec![42, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			ti.write::<u8>(5, u8::MAX).unwrap();
			assert_eq!(ti.data, vec![42, 0, 0, 0, 0, u8::MAX, 0, 0, 0, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<u8>(10, 42);
			assert!(result.is_err());

			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod i16 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i16>(2).unwrap();
			assert_eq!(result, 1027)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i16>(9);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(9))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<i16>(0, i16::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 127, 0, 0, 0, 0, 0, 0, 0, 0]);

			ti.write::<i16>(5, i16::MIN).unwrap();
			assert_eq!(ti.data, vec![255, 127, 0, 0, 0, 0, 128, 0, 0, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<i16>(10, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<i16>(9, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod u16 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u16>(2).unwrap();
			assert_eq!(result, 1027)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u16>(9);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(9))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<u16>(0, u16::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 0, 0, 0, 0, 0, 0, 0, 0]);

			ti.write::<u16>(5, 42).unwrap();
			assert_eq!(ti.data, vec![255, 255, 0, 0, 0, 42, 0, 0, 0, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<u16>(10, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<u16>(9, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod i32 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i32>(2).unwrap();
			assert_eq!(result, 100992003)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i32>(7);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(7))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<i32>(0, i32::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 127, 0, 0, 0, 0, 0, 0]);

			ti.write::<i32>(5, i32::MIN).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 127, 0, 0, 0, 0, 128, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<i32>(10, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<i32>(7, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod u32 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u32>(2).unwrap();
			assert_eq!(result, 100992003)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u32>(7);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(7))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<u32>(0, u32::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 255, 0, 0, 0, 0, 0, 0]);

			ti.write::<u32>(5, 42).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 255, 0, 42, 0, 0, 0, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<u32>(10, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<u32>(7, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod i64 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i64>(2).unwrap();
			assert_eq!(result, 2542101049181187)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<i64>(3);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(3))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<i64>(0, i64::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 255, 255, 255, 255, 127, 0, 0]);

			ti.write::<i64>(2, i64::MIN).unwrap();
			assert_eq!(ti.data, vec![255, 255, 0, 0, 0, 0, 0, 0, 0, 128]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<i64>(10, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<i64>(7, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod u64 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u64>(2).unwrap();
			assert_eq!(result, 2542101049181187)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<u64>(3);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(3))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<u64>(0, u64::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0]);

			ti.write::<u64>(2, 42).unwrap();
			assert_eq!(ti.data, vec![255, 255, 42, 0, 0, 0, 0, 0, 0, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<u64>(10, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<u64>(3, 42);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod f32 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<f32>(2).unwrap();
			assert_eq!(result, 2.5017467e-35)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<f32>(7);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(7))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<f32>(0, f32::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 127, 127, 0, 0, 0, 0, 0, 0]);

			ti.write::<f32>(5, 42f32).unwrap();
			assert_eq!(ti.data, vec![255, 255, 127, 127, 0, 0, 0, 40, 66, 0]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<f32>(10, 42f32);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<f32>(7, 42f32);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}

	mod f64 {
		use crate::module::{Memory, Trap, TrapOutOfRange};

		#[test]
		fn read() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<f64>(2).unwrap();
			assert_eq!(result, 1.2559647966574136e-308)
		}

		#[test]
		fn reads_out_of_range() {
			let ti = Memory {
				data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
				max: None,
			};
			let result = ti.read::<f64>(3);
			assert_eq!(result, Err(Trap::OutOfRange(TrapOutOfRange::Memory(3))))
		}

		#[test]
		fn write() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			ti.write::<f64>(0, f64::MAX).unwrap();
			assert_eq!(ti.data, vec![255, 255, 255, 255, 255, 255, 239, 127, 0, 0]);

			ti.write::<f64>(2, 42f64).unwrap();
			assert_eq!(ti.data, vec![255, 255, 0, 0, 0, 0, 0, 0, 69, 64]);
		}

		#[test]
		fn write_out_of_range() {
			let mut ti = Memory {
				data: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
				max: None,
			};
			let result = ti.write::<f64>(10, 42f64);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

			let result = ti.write::<f64>(3, 42f64);
			assert!(result.is_err());
			assert_eq!(ti.data, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		}
	}
}
