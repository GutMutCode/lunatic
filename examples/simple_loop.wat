;; Simple loop demo - runs indefinitely printing messages
(module
  (import "wasi_snapshot_preview1" "fd_write"
    (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit"
    (func $proc_exit (param i32)))
  
  (memory (export "memory") 1)
  
  ;; Message: "Running v1...\n" at offset 100
  (data (i32.const 100) "Running v1...\0a")
  
  ;; Counter at offset 0
  (data (i32.const 0) "\00\00\00\00")
  
  (func (export "_start")
    (local $counter i32)
    (local $i i32)
    
    ;; Infinite loop
    (block $break
      (loop $continue
        ;; Load and increment counter
        (local.set $counter (i32.load (i32.const 0)))
        (local.set $counter (i32.add (local.get $counter) (i32.const 1)))
        (i32.store (i32.const 0) (local.get $counter))
        
        ;; Print message
        (call $print_message)
        
        ;; Simple delay loop (longer delay)
        (local.set $i (i32.const 0))
        (block $delay_break
          (loop $delay_continue
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $delay_continue 
              (i32.lt_u (local.get $i) (i32.const 100000000)))
          )
        )
        
        ;; Exit after 100 iterations (more time to test)
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
    ;; iov[0].buf = 100 (message start)
    (i32.store (i32.const 200) (i32.const 100))
    ;; iov[0].len = 14 (message length)
    (i32.store (i32.const 204) (i32.const 14))
    
    ;; fd_write(stdout=1, iov=200, iovcnt=1, nwritten=300)
    (drop (call $fd_write
      (i32.const 1)
      (i32.const 200)
      (i32.const 1)
      (i32.const 300)
    ))
  )
)
