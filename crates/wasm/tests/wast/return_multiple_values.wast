(module
    (func (export "1") (param i32 i64) (result i32 i64)
    ;; Here we do some operations, for example, returning the inputs
    (local.get 0)  ;; Return the first input
    (local.get 1)  ;; Return the second input
    )

    (func (export "2") (result i32 i64)
      (i32.const 2)
      (i64.const 7)
    )
)


(assert_return
    (invoke "1" (i32.const 10) (i64.const 20))
    (i32.const 10) (i64.const 20)
)


(assert_return
    (invoke "2")
    (i32.const 2) (i64.const 7)
)