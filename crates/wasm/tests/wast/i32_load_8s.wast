(module
  ;; Define a memory of 1 page (64KiB)
  (memory 1)

  ;; Store 8-bit values in memory at different locations
  (data (i32.const 0) "\7F")  ;; 127 in decimal, 0x7F (positive)
  (data (i32.const 1) "\80")  ;; -128 in decimal, 0x80 (negative)

  ;; Function to load 8-bit signed integer at an offset and return the result
  (func (export "load_signed_127") (result i32)
    (i32.load8_s (i32.const 0))) ;; Load from address 0 (0x7F, positive)

  (func (export "load_signed_neg_128") (result i32)
    (i32.load8_s (i32.const 1))) ;; Load from address 1 (0x80, negative)
)

;; Test case for loading the value 127 (0x7F) from memory
(assert_return (invoke "load_signed_127") (i32.const 127))

;; Test case for loading the value -128 (0x80) from memory
(assert_return (invoke "load_signed_neg_128") (i32.const -128))
