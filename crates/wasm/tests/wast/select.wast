(module
  (func (export "select-i32") (param i32 i32 i32) (result i32)
    (select (local.get 0) (local.get 1) (local.get 2))
  )
)

(assert_return (invoke "select-i32" (i32.const 1) (i32.const 2) (i32.const 1)) (i32.const 1))
