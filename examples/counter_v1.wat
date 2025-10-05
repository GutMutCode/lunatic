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
    (i32.const 0)
    (i32.const 0)
    (i32.load)
    ;; Add 1
    (i32.const 1)
    (i32.add)
    ;; Store back (pops value then address)
    (i32.store)
    ;; Return new value
    (i32.const 0)
    (i32.load)
  )
  
  ;; Get current counter value
  (func (export "get_count") (result i32)
    (i32.const 0)
    (i32.load)
  )
  
  ;; Required entry point
  (func (export "_start")
    ;; Do nothing, just satisfy WASI requirements
    nop
  )
)
