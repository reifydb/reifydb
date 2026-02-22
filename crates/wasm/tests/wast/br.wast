(module
  (func (export "as-return-values") (result i32 i64)
    (i32.const 2)
    (block (result i64) (return (br 0 (i32.const 1) (i64.const 7))))
  )
)


(assert_return (invoke "as-return-values") (i32.const 2) (i64.const 7))