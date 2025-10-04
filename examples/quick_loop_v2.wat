;; Quick loop demo v2 - modified message
(module
  (import "wasi_snapshot_preview1" "fd_write"
    (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit"
    (func $proc_exit (param i32)))
  
  (memory (export "memory") 1)
  
  ;; NEW MESSAGE: "RELOADED v2!\n" at offset 100
  (data (i32.const 100) "RELOADED v2!\0a")
  
  ;; Counter at offset 0
  (data (i32.const 0) "\00\00\00\00")
  
  (func (export "_start")
    (local $counter i32)
    (local $i i32)
    
    ;; Infinite loop
    (block $break
      (loop $continue
        ;; Load and increment counter BY 2 in v2!
        (local.set $counter (i32.load (i32.const 0)))
        (local.set $counter (i32.add (local.get $counter) (i32.const 2)))
        (i32.store (i32.const 0) (local.get $counter))
        
        ;; Print message
        (call $print_message)
        
        ;; Much shorter delay (10 million)
        (local.set $i (i32.const 0))
        (block $delay_break
          (loop $delay_continue
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $delay_continue 
              (i32.lt_u (local.get $i) (i32.const 10000000)))
          )
        )
        
        ;; Exit after 100 iterations
        (br_if $break (i32.gt_u (local.get $counter) (i32.const 100)))
        
        ;; Continue loop
        (br $continue)
      )
    )
    
    ;; Exit cleanly
    (call $proc_exit (i32.const 0))
  )
  
  (func $print_message
    ;; Setup iovec at offset 200
    (i32.store (i32.const 200) (i32.const 100))
    (i32.store (i32.const 204) (i32.const 13))
    
    ;; fd_write
    (drop (call $fd_write
      (i32.const 1)
      (i32.const 200)
      (i32.const 1)
      (i32.const 300)
    ))
  )
)
