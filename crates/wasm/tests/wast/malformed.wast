(assert_malformed
  (module quote "(func (result i32) (i32.const nan:arithmetic))")
  "unexpected token"
)