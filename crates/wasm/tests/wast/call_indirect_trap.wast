(module
  (type $ii-i (func (param i32 i32) (result i32)))

  (table $t3 4 funcref)
  (elem (table $t3) (i32.const 0) func $g $h)
  (elem (table $t3) (i32.const 3) func $z)

  (func $g (type $ii-i) (i32.sub (local.get 0) (local.get 1)))
  (func $h (type $ii-i) (i32.mul (local.get 0) (local.get 1)))
  (func $z)

  (func (export "call-3") (param i32 i32 i32) (result i32)
    (call_indirect $t3 (type $ii-i) (local.get 0) (local.get 1) (local.get 2))
  )
)

(assert_trap (invoke "call-3" (i32.const 2) (i32.const 3) (i32.const 2)) "uninitialized element")
(assert_trap (invoke "call-3" (i32.const 2) (i32.const 3) (i32.const 3)) "indirect call type mismatch")
(assert_trap (invoke "call-3" (i32.const 2) (i32.const 3) (i32.const 4)) "undefined element")
