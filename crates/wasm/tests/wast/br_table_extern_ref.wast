(module
  (func (export "meet-externref") (param i32) (param externref) (result externref)
    (block $l1 (result externref)
      (block $l2 (result externref)
        (br_table $l1 $l2 $l1 (local.get 1) (local.get 0))
      )
    )
  )
)


(assert_return (invoke "meet-externref" (i32.const 0) (ref.extern 1)) (ref.extern 1))
(assert_return (invoke "meet-externref" (i32.const 1) (ref.extern 1)) (ref.extern 1))
(assert_return (invoke "meet-externref" (i32.const 2) (ref.extern 1)) (ref.extern 1))
