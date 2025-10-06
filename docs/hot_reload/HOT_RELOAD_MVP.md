# Hot Reload MVP Implementation

## Overview

This is the MVP (Minimum Viable Product) implementation of hot reloading for Lunatic runtime. It provides automatic process restart when WebAssembly files change during development.

## Features

- **File Watching**: Automatically detects changes to `.wasm` files
- **Process Restart**: Restarts the process when changes are detected
- **Development Mode**: Enabled with the `--watch` flag
- **Simple & Fast**: No state preservation (restart-based approach)

## Usage

### Basic Example

```bash
# Run your wasm module in watch mode
lunatic run --watch myapp.wasm

# Or with additional arguments
lunatic run --watch myapp.wasm arg1 arg2

# With directory access
lunatic run --watch --dir /path/to/dir myapp.wasm
```

### Development Workflow

1. Start your application in watch mode:
   ```bash
   lunatic run --watch app.wasm
   ```

2. Edit your source code

3. Recompile to WebAssembly:
   ```bash
   cargo build --target wasm32-wasi
   ```

4. Lunatic automatically detects the change and restarts your application

## Architecture

### Components

1. **FileWatcher** (`src/hot_reload/watcher.rs`)
   - Uses the `notify` crate for cross-platform file watching
   - Monitors `.wasm` files for modifications
   - Sends events through a tokio channel

2. **Process Manager** (`src/mode/run.rs`)
   - Receives file change events
   - Aborts the current process
   - Spawns a new process with the updated code

### Flow Diagram

```
┌─────────────────┐
│  File System    │
│   (.wasm file)  │
└────────┬────────┘
         │ change detected
         ▼
┌─────────────────┐
│  FileWatcher    │
│   (notify)      │
└────────┬────────┘
         │ FileChangeEvent
         ▼
┌─────────────────┐
│ Process Manager │
│                 │
│  1. Abort old   │
│  2. Start new   │
└─────────────────┘
```

## Limitations (MVP)

This MVP implementation has the following limitations:

1. **No State Preservation**: Process state is lost on reload
2. **Development Only**: Not recommended for production use
3. **Single File**: Watches only the main `.wasm` file
4. **Manual Recompilation**: You must recompile your code manually

## Future Enhancements

Potential improvements for future versions:

- **State Preservation**: Save and restore process state across reloads
- **Smart Reloading**: Only reload affected processes
- **Module Versioning**: Support multiple module versions simultaneously  
- **Distributed Reload**: Coordinate reloads across distributed nodes
- **Auto-recompilation**: Integrate with build tools (cargo watch)

## Implementation Details

### File Watching

Uses `notify::RecommendedWatcher` which selects the best backend for each platform:
- **macOS**: FSEvents
- **Linux**: inotify
- **Windows**: ReadDirectoryChangesW

### Event Filtering

Only triggers on:
- `Modify(Data)` events: File content changes
- `Create` events: New file creation

Ignores temporary files and non-`.wasm` files.

### Process Lifecycle

```rust
// Simplified pseudocode
loop {
    select! {
        // File changed
        event = rx.recv() => {
            abort_current_process();
            start_new_process();
        }
        // Process finished
        result = process.await => {
            if error {
                wait_for_changes();
            } else {
                exit();
            }
        }
    }
}
```

## Testing

To test the hot reload functionality:

1. Create a simple wasm module
2. Run it with `--watch`
3. Modify and recompile
4. Observe the automatic restart

## Contributing

This is an initial MVP. Contributions welcome for:
- Bug fixes
- Performance improvements
- Additional features (see Future Enhancements)
- Documentation improvements

## References

- [Erlang Hot Code Loading](https://www.erlang.org/doc/reference_manual/code_loading.html)
- [notify crate](https://docs.rs/notify/)
- [Lunatic Process Model](../README.md#architecture)
