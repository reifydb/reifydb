;; Test files can define multiple modules. Test that implementations treat
;; each module independently from the other.

(module
  (func (export "foo") (result i32) (i32.const 0))
)

(assert_return (invoke "foo") (i32.const 0))

;; Another module, same function name, different contents.

(module
  (func (export "foo") (result i32) (i32.const 1))
)

(assert_return (invoke "foo") (i32.const 1))
