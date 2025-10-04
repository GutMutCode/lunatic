;; Counter Module v1 - Simple counter
(module
  ;; Export memory so host can snapshot it
  (memory (export "memory") 1)
  
  ;; Counter stored at memory offset 0
  ;; Initialize counter to 0
  (data (i32.const 0) "\00\00\00\00")
  
  ;; Increment counter
  (func (export "increment") (result i32)
    ;; Load current value
    (i32.load (i32.const 0))
    ;; Add 1
    (i32.const 1)
    (i32.add)
    ;; Store back
    (i32.store (i32.const 0))
    ;; Return new value
    (i32.load (i32.const 0))
  )
  
  ;; Get current counter value
  (func (export "get_count") (result i32)
    (i32.load (i32.const 0))
  )
  
  ;; Required entry point
  (func (export "_start")
    ;; Do nothing, just satisfy WASI requirements
    nop
  )
)
