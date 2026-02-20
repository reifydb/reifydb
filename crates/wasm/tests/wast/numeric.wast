(module
  (func (export "add") (param $x f32) (param $y f32) (result f32) (f32.add (local.get $x) (local.get $y)))
  (func (export "sub") (param $x i32) (param $y i32) (result i32) (i32.sub (local.get $x) (local.get $y)))
)

(assert_return (invoke "add" (f32.const -0x0p+0) (f32.const -nan)) (f32.const nan:canonical))
(assert_return (invoke "add" (f32.const -0x0p+0) (f32.const -nan:0x200000)) (f32.const nan:arithmetic))
(assert_return (invoke "add" (f32.const -0x0p+0) (f32.const nan)) (f32.const nan:canonical))
(assert_return (invoke "add" (f32.const -0x0p+0) (f32.const nan:0x200000)) (f32.const nan:arithmetic))

(assert_return (invoke "sub" (i32.const 1) (i32.const 0)) (i32.const 1))

(module
  (func (export "min") (param $x f64) (param $y f64) (result f64) (f64.min (local.get $x) (local.get $y)))
)


(assert_return (invoke "min" (f64.const -0x0p+0) (f64.const 0x0p+0)) (f64.const -0x0p+0))
(assert_return (invoke "min" (f64.const 0x0p+0) (f64.const -0x0p+0)) (f64.const -0x0p+0))




(assert_return (invoke "min" (f64.const -0x0.0000000000001p-1022) (f64.const -0x0p+0)) (f64.const -0x0.0000000000001p-1022))

