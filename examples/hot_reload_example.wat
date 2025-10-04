(module
  (import "wasi_snapshot_preview1" "proc_exit"
    (func $proc_exit (param i32)))
  
  (func (export "_start")
    ;; Print message (simplified - actual implementation would use fd_write)
    ;; For now just exit successfully
    (call $proc_exit (i32.const 0))
  )
  
  (memory (export "memory") 1)
)
