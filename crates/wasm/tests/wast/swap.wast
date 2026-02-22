(module
    (func (export "swap") (param $a i32) (param $b i32) (result i32 i32)
      local.get $b   ;; Push the second parameter onto the stack
      local.get $a   ;; Push the first parameter onto the stack
      ;; The stack now has the values [$b, $a]
      ;; These will be returned as the results
    )

    (func (export "swap_2") (param i32) (param i32) (result i32 i32)
      local.get 1   ;; Push the second parameter onto the stack
      local.get 0   ;; Push the first parameter onto the stack
    )
)

(assert_return
    (invoke "swap" (i32.const 1) (i32.const 2))
    (i32.const 2) (i32.const 1)
)

(assert_return
    (invoke "swap_2" (i32.const 1) (i32.const 2))
    (i32.const 2) (i32.const 1)
)