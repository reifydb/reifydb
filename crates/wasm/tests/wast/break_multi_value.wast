(module
  ;; Auxiliary definition
  (memory 1)

  (func (export "break-multi-value") (result i32 i32 i64)
    (block (result i32 i32 i64)
      (br 0 (i32.const 18) (i32.const -18) (i64.const 18))
      (i32.const 19) (i32.const -19) (i64.const 19)
    )
  )
)

(assert_return (invoke "break-multi-value")
  (i32.const 18) (i32.const -18) (i64.const 18)
)