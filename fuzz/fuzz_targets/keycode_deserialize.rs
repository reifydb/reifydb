#![no_main]

use libfuzzer_sys::fuzz_target;
use reifydb_core::util::encoding::keycode;

fuzz_target!(|data: &[u8]| {
    let _ = keycode::deserialize::<bool>(data);
    let _ = keycode::deserialize::<u8>(data);
    let _ = keycode::deserialize::<u16>(data);
    let _ = keycode::deserialize::<u32>(data);
    let _ = keycode::deserialize::<u64>(data);
    let _ = keycode::deserialize::<u128>(data);
    let _ = keycode::deserialize::<i8>(data);
    let _ = keycode::deserialize::<i16>(data);
    let _ = keycode::deserialize::<i32>(data);
    let _ = keycode::deserialize::<i64>(data);
    let _ = keycode::deserialize::<i128>(data);
    let _ = keycode::deserialize::<f32>(data);
    let _ = keycode::deserialize::<f64>(data);
    let _ = keycode::deserialize::<String>(data);
    let _ = keycode::deserialize::<Option<i64>>(data);
    let _ = keycode::deserialize::<(bool, u64)>(data);
});
