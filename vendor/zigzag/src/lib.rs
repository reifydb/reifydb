use {
    num_traits::{AsPrimitive, One, PrimInt, Signed, Unsigned},
    std::mem::size_of,
};

pub trait ZigZag: Signed + PrimInt
where
    Self: AsPrimitive<<Self as ZigZag>::UInt>,
{
    type UInt: Unsigned + PrimInt + AsPrimitive<Self>;
    #[inline]
    fn encode(value: Self) -> Self::UInt {
        let s = (value << 1) ^ (value >> ((size_of::<Self>() * 8) - 1));
        AsPrimitive::<Self::UInt>::as_(s)
    }
    #[inline]
    fn decode(value: Self::UInt) -> Self {
        let shr1 = value >> 1;
        let a1: Self = (value & One::one()).as_();
        let neg: Self::UInt = (-a1).as_();
        let or = shr1 ^ neg;
        or.as_()
    }
}

impl ZigZag for i128 {
    type UInt = u128;
}

impl ZigZag for i64 {
    type UInt = u64;
}

impl ZigZag for i32 {
    type UInt = u32;
}

impl ZigZag for i16 {
    type UInt = u16;
}

impl ZigZag for i8 {
    type UInt = u8;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn known_values() {
        let result = ZigZag::encode(0i32);
        assert_eq!(result, 0);

        let result = ZigZag::encode(-1i32);
        assert_eq!(result, 1);

        let result = ZigZag::encode(2147483647i32);
        assert_eq!(result, 4294967294);

        let result = ZigZag::encode(-2147483648i32);
        assert_eq!(result, 4294967295);
    }

    #[test]
    fn all_values() {
        for i in std::i16::MIN..=std::i16::MAX {
            let zig = ZigZag::encode(i);
            let zag = ZigZag::decode(zig);
            assert_eq!(i, zag);
        }
    }
}
