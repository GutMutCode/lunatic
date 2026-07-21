# Wasmtime fiber 8.0.1 compatibility backport

This directory contains the source of `wasmtime-fiber` 8.0.1 under its
upstream Apache-2.0 WITH LLVM-exception license. The workspace patches the
crates.io package to this copy because Lunatic is still built against the
Wasmtime 8 API.

The only source change is the Windows guard in `src/windows.rs`, backported
from Bytecode Alliance Wasmtime pull request #12426, commit
`aedc54800061b3cc028c6edb4ec30fdfaf89e4d6`. Rust 1.95 changed Windows
thread-local destruction to use Fiber Local Storage. Touching a destructor-
bearing thread-local before `ConvertThreadToFiber` ensures Rust's cleanup hook
belongs to the original thread instead of a child Wasmtime fiber. Without the
guard, cancelling a suspended `call_async` future can abort the process while
the fiber is deleted.

Remove this patch when the workspace upgrades to a Wasmtime release that
contains the upstream fix.
