;; Counter Module v2 - Counter with double increment
(module
  ;; Export memory so host can snapshot it
  (memory (export "memory") 1)
  
  ;; Counter stored at memory offset 0
  ;; Initialize counter to 0
  (data (i32.const 0) "\00\00\00\00")
  
  ;; Increment counter by 2 (NEW in v2)
  (func (export "increment") (result i32)
    ;; Load current value
    (i32.const 0)
    (i32.const 0)
    (i32.load)
    ;; Add 2 instead of 1
    (i32.const 2)
    (i32.add)
    ;; Store back (pops value then address)
    (i32.store)
    ;; Return new value
    (i32.const 0)
    (i32.load)
  )
  
  ;; Get current counter value (unchanged)
  (func (export "get_count") (result i32)
    (i32.const 0)
    (i32.load)
  )
  
  ;; NEW in v2: Reset counter
  (func (export "reset")
    (i32.store (i32.const 0) (i32.const 0))
  )
  
  ;; Required entry point
  (func (export "_start")
    nop
  )
)
