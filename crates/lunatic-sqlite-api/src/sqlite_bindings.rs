use anyhow::Result;
use hash_map_id::HashMapId;
use lunatic_common_api::{get_memory, write_to_guest_vec, IntoTrap, LinkerAsyncExt};
use lunatic_error_api::ErrorCtx;
use lunatic_process::state::ProcessState;
use lunatic_process_api::ProcessConfigCtx;
use sqlite::{Connection, State, Statement};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    ops::{Deref, Range},
    path::Path,
    sync::{Arc, Mutex},
};
use wasmtime::{Caller, Linker, ResourceLimiter, ToWasmtimeResult as _};

use crate::wire_format::{BindList, SqliteError, SqliteRow, SqliteValue};

pub const SQLITE_ROW: u32 = 100;
pub const SQLITE_DONE: u32 = 101;
pub const DEFAULT_MAX_SQLITE_CONNECTIONS: u32 = 64;
pub const DEFAULT_MAX_SQLITE_STATEMENTS: u32 = 256;

pub trait SQLiteResourceQuota: Send + Sync {
    fn reserve_connection(&self) -> Result<()>;
    fn release_connection(&self) -> Result<()>;
    fn reserve_statement(&self) -> Result<()>;
    fn release_statement(&self) -> Result<()>;
}

#[derive(Debug)]
struct SQLiteResourceCounts {
    open_connections: u32,
    open_statements: u32,
    max_connections: u32,
    max_statements: u32,
}

#[derive(Debug)]
pub struct SQLiteResourceStats {
    counts: Mutex<SQLiteResourceCounts>,
}

impl SQLiteResourceStats {
    pub fn new(max_connections: u32, max_statements: u32) -> Self {
        Self {
            counts: Mutex::new(SQLiteResourceCounts {
                open_connections: 0,
                open_statements: 0,
                max_connections,
                max_statements,
            }),
        }
    }

    pub fn counts(&self) -> (u32, u32) {
        let counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        (counts.open_connections, counts.open_statements)
    }

    pub fn validate_limits(&self, max_connections: u32, max_statements: u32) -> Result<()> {
        let counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        anyhow::ensure!(
            counts.open_connections <= max_connections,
            "{} live SQLite connections exceed replacement limit {}",
            counts.open_connections,
            max_connections
        );
        anyhow::ensure!(
            counts.open_statements <= max_statements,
            "{} live SQLite statements exceed replacement limit {}",
            counts.open_statements,
            max_statements
        );
        Ok(())
    }

    pub fn set_limits(&self, max_connections: u32, max_statements: u32) {
        let mut counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        debug_assert!(counts.open_connections <= max_connections);
        debug_assert!(counts.open_statements <= max_statements);
        counts.max_connections = max_connections;
        counts.max_statements = max_statements;
    }
}

impl Default for SQLiteResourceStats {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_SQLITE_CONNECTIONS,
            DEFAULT_MAX_SQLITE_STATEMENTS,
        )
    }
}

impl SQLiteResourceQuota for SQLiteResourceStats {
    fn reserve_connection(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        anyhow::ensure!(
            counts.open_connections < counts.max_connections,
            "Max SQLite connections ({}) reached",
            counts.max_connections
        );
        counts.open_connections += 1;
        Ok(())
    }

    fn release_connection(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        anyhow::ensure!(
            counts.open_connections > 0,
            "SQLite connection accounting underflow"
        );
        counts.open_connections -= 1;
        Ok(())
    }

    fn reserve_statement(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        anyhow::ensure!(
            counts.open_statements < counts.max_statements,
            "Max SQLite statements ({}) reached",
            counts.max_statements
        );
        counts.open_statements += 1;
        Ok(())
    }

    fn release_statement(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("SQLite resource accounting mutex poisoned");
        anyhow::ensure!(
            counts.open_statements > 0,
            "SQLite statement accounting underflow"
        );
        counts.open_statements -= 1;
        Ok(())
    }
}

enum SQLiteResourceKind {
    Connection,
    Statement,
}

struct SQLiteResourceLease {
    quota: Arc<dyn SQLiteResourceQuota>,
    kind: SQLiteResourceKind,
}

impl SQLiteResourceLease {
    fn connection(quota: Arc<dyn SQLiteResourceQuota>) -> Result<Self> {
        quota.reserve_connection()?;
        Ok(Self {
            quota,
            kind: SQLiteResourceKind::Connection,
        })
    }

    fn statement(quota: Arc<dyn SQLiteResourceQuota>) -> Result<Self> {
        quota.reserve_statement()?;
        Ok(Self {
            quota,
            kind: SQLiteResourceKind::Statement,
        })
    }
}

impl Drop for SQLiteResourceLease {
    fn drop(&mut self) {
        let result = match self.kind {
            SQLiteResourceKind::Connection => self.quota.release_connection(),
            SQLiteResourceKind::Statement => self.quota.release_statement(),
        };
        debug_assert!(result.is_ok());
    }
}

pub struct SQLiteConnectionResource {
    connection: Mutex<Connection>,
    _lease: SQLiteResourceLease,
}

impl SQLiteConnectionResource {
    fn new(connection: Connection, lease: SQLiteResourceLease) -> Self {
        Self {
            connection: Mutex::new(connection),
            _lease: lease,
        }
    }
}

impl Deref for SQLiteConnectionResource {
    type Target = Mutex<Connection>;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl fmt::Debug for SQLiteConnectionResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SQLiteConnectionResource")
            .finish_non_exhaustive()
    }
}

pub struct SQLiteStatementResource {
    // Drop the native statement before its connection and both quota leases.
    statement: Statement,
    connection_id: u64,
    _connection: Arc<SQLiteConnectionResource>,
    _lease: SQLiteResourceLease,
}

impl fmt::Debug for SQLiteStatementResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SQLiteStatementResource")
            .field("connection_id", &self.connection_id)
            .finish_non_exhaustive()
    }
}

pub type SQLiteConnections = HashMapId<Arc<SQLiteConnectionResource>>;
pub type SQLiteResults = HashMapId<Vec<u8>>;
pub type SQLiteStatements = HashMapId<SQLiteStatementResource>;
// maps connection_id to name of allocation function
pub type SQLiteGuestAllocators = HashMap<u64, String>;
pub trait SQLiteCtx {
    fn sqlite_connections(&self) -> &SQLiteConnections;
    fn sqlite_connections_mut(&mut self) -> &mut SQLiteConnections;

    fn sqlite_guest_allocator(&self) -> &SQLiteGuestAllocators;
    fn sqlite_guest_allocator_mut(&mut self) -> &mut SQLiteGuestAllocators;

    fn sqlite_statements(&self) -> &SQLiteStatements;
    fn sqlite_statements_mut(&mut self) -> &mut SQLiteStatements;

    fn sqlite_quota(&self) -> Arc<dyn SQLiteResourceQuota>;
}

// Register the SqlLite apis
pub fn register<T: SQLiteCtx + ProcessState + Send + ErrorCtx + ResourceLimiter + Sync + 'static>(
    linker: &mut Linker<T>,
) -> Result<()>
where
    T::Config: lunatic_process_api::ProcessConfigCtx,
{
    linker.func_wrap(
        "lunatic::sqlite",
        "open",
        |caller: Caller<'_, T>, path_str_ptr: u32, path_str_len: u32, connection_id_ptr: u32| {
            open(caller, path_str_ptr, path_str_len, connection_id_ptr).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "query_prepare",
        |caller: Caller<'_, T>, connection_id: u64, query_ptr: u32, query_len: u32| {
            query_prepare(caller, connection_id, query_ptr, query_len).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "query_prepare_checked",
        |caller: Caller<'_, T>,
         connection_id: u64,
         query_ptr: u32,
         query_len: u32,
         statement_id_ptr: u32| {
            query_prepare_checked(
                caller,
                connection_id,
                query_ptr,
                query_len,
                statement_id_ptr,
            )
            .to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "execute",
        |caller: Caller<'_, T>, connection_id: u64, query_ptr: u32, query_len: u32| {
            execute(caller, connection_id, query_ptr, query_len).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "bind_value",
        |caller: Caller<'_, T>, statement_id: u64, bind_ptr: u32, bind_len: u32| {
            bind_value(caller, statement_id, bind_ptr, bind_len).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "sqlite3_changes",
        |caller: Caller<'_, T>, connection_id: u64| {
            sqlite3_changes(caller, connection_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "statement_reset",
        |caller: Caller<'_, T>, statement_id: u64| {
            statement_reset(caller, statement_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async("lunatic::sqlite", "last_error", last_error)?;
    linker.func_wrap(
        "lunatic::sqlite",
        "sqlite3_finalize",
        |caller: Caller<'_, T>, statement_id: u64| {
            sqlite3_finalize(caller, statement_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "sqlite3_close",
        |caller: Caller<'_, T>, connection_id: u64| {
            sqlite3_close(caller, connection_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::sqlite",
        "sqlite3_step",
        |caller: Caller<'_, T>, statement_id: u64| {
            sqlite3_step(caller, statement_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap3_async("lunatic::sqlite", "read_column", read_column)?;
    linker.func_wrap2_async("lunatic::sqlite", "column_names", column_names)?;
    linker.func_wrap2_async("lunatic::sqlite", "read_row", read_row)?;
    linker.func_wrap(
        "lunatic::sqlite",
        "column_count",
        |caller: Caller<'_, T>, statement_id: u64| {
            column_count(caller, statement_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap3_async("lunatic::sqlite", "column_name", column_name)?;
    Ok(())
}

fn checked_guest_range(pointer: u32, length: u32, operation: &'static str) -> Result<Range<usize>> {
    let end = pointer
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("{operation}: guest pointer overflow"))?;
    Ok(pointer as usize..end as usize)
}

fn open<T>(
    mut caller: Caller<T>,
    path_str_ptr: u32,
    path_str_len: u32,
    connection_id_ptr: u32,
) -> Result<u64>
where
    T: ProcessState + ErrorCtx + SQLiteCtx,
    T::Config: lunatic_process_api::ProcessConfigCtx,
{
    let memory = get_memory(&mut caller)?;
    let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
    let output_range = checked_guest_range(
        connection_id_ptr,
        std::mem::size_of::<u64>() as u32,
        "lunatic::sqlite::open",
    )?;
    memory_slice
        .get(output_range.clone())
        .or_trap("lunatic::sqlite::open: connection result pointer is out of bounds")?;
    let path = memory_slice
        .get(checked_guest_range(
            path_str_ptr,
            path_str_len,
            "lunatic::sqlite::open",
        )?)
        .or_trap("lunatic::sqlite::open")?;
    let path = std::str::from_utf8(path).or_trap("lunatic::sqlite::open")?;

    if let Err(error_message) = state.config().can_access_fs_location(Path::new(path)) {
        let error_id = state.add_error_resource(
            anyhow::Error::msg(error_message).context(format!("Failed to access '{path}'")),
        );
        memory_slice[output_range].copy_from_slice(&error_id.to_le_bytes());
        return Ok(1);
    }

    let lease = match SQLiteResourceLease::connection(state.sqlite_quota()) {
        Ok(lease) => lease,
        Err(error) => {
            let error_id = state.add_error_resource(error);
            memory_slice[output_range].copy_from_slice(&error_id.to_le_bytes());
            return Ok(1);
        }
    };
    let (conn_or_err_id, return_code) = match sqlite::open(path) {
        Ok(connection) => (
            state
                .sqlite_connections_mut()
                .add(Arc::new(SQLiteConnectionResource::new(connection, lease))),
            0,
        ),
        Err(error) => (state.add_error_resource(error.into()), 1),
    };

    memory_slice[output_range].copy_from_slice(&conn_or_err_id.to_le_bytes());
    Ok(return_code)
}

fn execute<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    conn_id: u64,
    exec_str_ptr: u32,
    exec_str_len: u32,
) -> Result<u32> {
    let memory = get_memory(&mut caller)?;
    let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
    let exec = memory_slice
        .get(exec_str_ptr as usize..(exec_str_ptr + exec_str_len) as usize)
        .or_trap("lunatic::sqlite::execute")?;
    let exec = std::str::from_utf8(exec).or_trap("lunatic::sqlite::execute")?;

    // execute a single sqlite query
    match state
        .sqlite_connections()
        .get(conn_id)
        .or_trap("lunatic::sqlite::execute")?
        .lock()
        .or_trap("lunatic::sqlite::execute")?
        .execute(exec)
    {
        // 1 is equal to SQLITE_ERROR, which is a generic error code
        Err(e) => Ok(e.code.unwrap_or(1) as u32),
        Ok(_) => Ok(0),
    }
}

fn query_prepare<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    conn_id: u64,
    query_str_ptr: u32,
    query_str_len: u32,
) -> Result<u64> {
    // get the memory
    let memory = get_memory(&mut caller)?;
    let (memory_slice, state) = memory.data_and_store_mut(&mut caller);

    // get the query
    let query = memory_slice
        .get(checked_guest_range(
            query_str_ptr,
            query_str_len,
            "lunatic::sqlite::query_prepare",
        )?)
        .or_trap("lunatic::sqlite::query_prepare::get_query")?;
    let query = std::str::from_utf8(query).or_trap("lunatic::sqlite::query_prepare::from_utf8")?;

    prepare_statement(state, conn_id, query)
}

fn prepare_statement<T: SQLiteCtx>(state: &mut T, conn_id: u64, query: &str) -> Result<u64> {
    let lease = SQLiteResourceLease::statement(state.sqlite_quota())?;
    let connection = state
        .sqlite_connections()
        .get(conn_id)
        .as_ref()
        .map(|resource| (*resource).clone())
        .ok_or_else(|| anyhow::anyhow!("SQLite connection ID {conn_id} does not exist"))?;
    let statement = {
        let conn = connection
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite connection mutex poisoned"))?;
        conn.prepare(query).map_err(anyhow::Error::from)?
    };

    Ok(state.sqlite_statements_mut().add(SQLiteStatementResource {
        statement,
        connection_id: conn_id,
        _connection: connection,
        _lease: lease,
    }))
}

fn query_prepare_checked<T>(
    mut caller: Caller<T>,
    conn_id: u64,
    query_str_ptr: u32,
    query_str_len: u32,
    statement_id_ptr: u32,
) -> Result<i64>
where
    T: ProcessState + ErrorCtx + SQLiteCtx,
{
    let memory = get_memory(&mut caller)?;
    let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
    let output_range = checked_guest_range(
        statement_id_ptr,
        std::mem::size_of::<u64>() as u32,
        "lunatic::sqlite::query_prepare_checked",
    )?;
    memory_slice
        .get(output_range.clone())
        .or_trap("lunatic::sqlite::query_prepare_checked: result pointer is out of bounds")?;
    let query = memory_slice
        .get(checked_guest_range(
            query_str_ptr,
            query_str_len,
            "lunatic::sqlite::query_prepare_checked",
        )?)
        .or_trap("lunatic::sqlite::query_prepare_checked: query is out of bounds")?;
    let query = std::str::from_utf8(query)
        .or_trap("lunatic::sqlite::query_prepare_checked: query is not UTF-8")?
        .to_owned();

    match prepare_statement(state, conn_id, &query) {
        Ok(statement_id) => {
            memory_slice[output_range].copy_from_slice(&statement_id.to_le_bytes());
            Ok(-1)
        }
        Err(error) => Ok(state.add_error_resource(error) as i64),
    }
}

macro_rules! get_statement {
    ($state:ident, $statement_id:ident) => {
        $state
            .sqlite_statements_mut()
            .get_mut($statement_id)
            .map(|resource| (resource.connection_id, &mut resource.statement))
            .or_trap("lunatic::sqlite::get_statement_by_id")?
    };
}

macro_rules! get_conn {
    ($state:ident, $conn_id:ident, $fn_name:literal) => {{
        let trap_str = concat!("lunatic::sqlite::", $fn_name, "::obtain_conn");
        $state
            .sqlite_connections_mut()
            .get($conn_id)
            .take()
            .or_trap(trap_str)?
            .lock()
            .or_trap(trap_str)?
    }};
}

fn bind_value<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    statement_id: u64,
    bind_data_ptr: u32,
    bind_data_len: u32,
) -> Result<()> {
    // get the memory
    let memory = get_memory(&mut caller)?;
    let (memory_slice, state) = memory.data_and_store_mut(&mut caller);

    let (_, statement) = get_statement!(state, statement_id);

    // get the query
    let bind_data = memory_slice
        .get(checked_guest_range(
            bind_data_ptr,
            bind_data_len,
            "lunatic::sqlite::bind_value",
        )?)
        .or_trap("lunatic::sqlite::bind_value::load_bind_data")?;

    let values: BindList =
        bincode::deserialize(bind_data).or_trap("lunatic::sqlite::bind_value::decode_bind_data")?;

    for pair in values.iter() {
        pair.bind(statement)
            .or_trap("lunatic::sqlite::bind_value")?;
    }

    Ok(())
}

fn sqlite3_changes<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    conn_id: u64,
) -> Result<u32> {
    // get state
    let memory = get_memory(&mut caller)?;
    let (_, state) = memory.data_and_store_mut(&mut caller);
    let conn = get_conn!(state, conn_id, "sqlite3_changes");

    Ok(conn.change_count() as u32)
}

fn statement_reset<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    statement_id: u64,
) -> Result<()> {
    // get state
    let memory = get_memory(&mut caller)?;
    let (_, state) = memory.data_and_store_mut(&mut caller);
    let (_, stmt) = get_statement!(state, statement_id);

    stmt.reset().or_trap("lunatic::sqlite::statement_reset")?;

    Ok(())
}

fn read_column<T: ProcessState + ErrorCtx + SQLiteCtx + Send + Sync>(
    mut caller: Caller<T>,
    statement_id: u64,
    col_idx: u32,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        // get state
        let memory = get_memory(&mut caller)?;
        let (_, state) = memory.data_and_store_mut(&mut caller);
        let (_, stmt) = get_statement!(state, statement_id);

        let column = bincode::serialize(&SqliteValue::read_column(stmt, col_idx as usize)?)
            .or_trap("lunatic::sqlite::read_column")?;

        write_to_guest_vec(&mut caller, &memory, &column, opaque_ptr).await
    })
}

fn column_names<T: ProcessState + ErrorCtx + SQLiteCtx + Send + Sync>(
    mut caller: Caller<T>,
    statement_id: u64,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        // get state
        let memory = get_memory(&mut caller)?;
        let (_, state) = memory.data_and_store_mut(&mut caller);
        let (_, stmt) = get_statement!(state, statement_id);

        let column_names = stmt.column_names().to_vec();

        let column_names =
            bincode::serialize(&column_names).or_trap("lunatic::sqlite::column_names")?;

        write_to_guest_vec(&mut caller, &memory, &column_names, opaque_ptr).await
    })
}

// this function assumes that the row has not been read yet and therefore
// starts at column_idx 0
fn read_row<T: ProcessState + ErrorCtx + SQLiteCtx + Send + Sync>(
    mut caller: Caller<T>,
    statement_id: u64,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        // get state
        let memory = get_memory(&mut caller)?;
        let (_, state) = memory.data_and_store_mut(&mut caller);
        let (_, stmt) = get_statement!(state, statement_id);

        let read_row = SqliteRow::read_row(stmt)?;

        let row = bincode::serialize(&read_row).or_trap("lunatic::sqlite::read_row")?;

        write_to_guest_vec(&mut caller, &memory, &row, opaque_ptr).await
    })
}

fn last_error<T: ProcessState + ErrorCtx + SQLiteCtx + ResourceLimiter + Send + Sync>(
    mut caller: Caller<T>,
    conn_id: u64,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        // get state
        let memory = get_memory(&mut caller)?;
        let err = {
            let (_, state) = memory.data_and_store_mut(&mut caller);
            let mut conn = get_conn!(state, conn_id, "last_error");

            let err: SqliteError = conn.last().or_trap("lunatic::sqlite::last_error")?.into();
            bincode::serialize(&err)
                .or_trap("lunatic::sqlite::last_error::encode_error_wire_format")?
        };

        write_to_guest_vec(&mut caller, &memory, &err, opaque_ptr).await
    })
}

fn sqlite3_finalize<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    statement_id: u64,
) -> Result<()> {
    // get state
    let memory = get_memory(&mut caller)?;
    let (_, state) = memory.data_and_store_mut(&mut caller);
    // dropping the statement should invoke the C function `sqlite3_finalize`
    state
        .sqlite_statements_mut()
        .remove(statement_id)
        .or_trap("lunatic::sqlite::sqlite3_finalize")?;

    Ok(())
}

fn sqlite3_close<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    connection_id: u64,
) -> Result<()> {
    caller
        .data_mut()
        .sqlite_connections_mut()
        .remove(connection_id)
        .or_trap("lunatic::sqlite::sqlite3_close")?;
    caller
        .data_mut()
        .sqlite_guest_allocator_mut()
        .remove(&connection_id);
    Ok(())
}

// sends back SQLITE_DONE or SQLITE_ROW depending on whether there's more data available or not
fn sqlite3_step<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    statement_id: u64,
) -> Result<u32> {
    // get state
    let memory = get_memory(&mut caller)?;
    let (_, state) = memory.data_and_store_mut(&mut caller);
    let (_, statement) = get_statement!(state, statement_id);

    match statement.next().or_trap("lunatic::sqlite::sqlite3_step")? {
        State::Done => Ok(SQLITE_DONE),
        State::Row => Ok(SQLITE_ROW),
    }
}

fn column_count<T: ProcessState + ErrorCtx + SQLiteCtx>(
    mut caller: Caller<T>,
    statement_id: u64,
) -> Result<u32> {
    // get state
    let memory = get_memory(&mut caller)?;
    let (_, state) = memory.data_and_store_mut(&mut caller);
    let (_, statement) = get_statement!(state, statement_id);

    Ok(statement.column_count() as u32)
}

fn column_name<T: ProcessState + ErrorCtx + SQLiteCtx + Send + Sync>(
    mut caller: Caller<T>,
    statement_id: u64,
    column_idx: u32,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        // get state
        let memory = get_memory(&mut caller)?;
        let (_, column_name) = {
            let (_, state) = memory.data_and_store_mut(&mut caller);
            let (connection_id, statement) = get_statement!(state, statement_id);

            (
                connection_id,
                statement
                    .column_name(column_idx as usize)
                    .or_trap("lunatic::sqlite::column_name")?
                    .to_owned(),
            )
        };

        write_to_guest_vec(&mut caller, &memory, column_name.as_bytes(), opaque_ptr).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCtx {
        connections: SQLiteConnections,
        statements: SQLiteStatements,
        allocators: SQLiteGuestAllocators,
        stats: Arc<SQLiteResourceStats>,
    }

    impl TestCtx {
        fn new(max_connections: u32, max_statements: u32) -> Self {
            Self {
                connections: SQLiteConnections::default(),
                statements: SQLiteStatements::default(),
                allocators: SQLiteGuestAllocators::default(),
                stats: Arc::new(SQLiteResourceStats::new(max_connections, max_statements)),
            }
        }

        fn open_memory_connection(&mut self) -> Result<u64> {
            let lease = SQLiteResourceLease::connection(self.sqlite_quota())?;
            let connection = sqlite::open(":memory:")?;
            Ok(self
                .connections
                .add(Arc::new(SQLiteConnectionResource::new(connection, lease))))
        }
    }

    impl SQLiteCtx for TestCtx {
        fn sqlite_connections(&self) -> &SQLiteConnections {
            &self.connections
        }

        fn sqlite_connections_mut(&mut self) -> &mut SQLiteConnections {
            &mut self.connections
        }

        fn sqlite_guest_allocator(&self) -> &SQLiteGuestAllocators {
            &self.allocators
        }

        fn sqlite_guest_allocator_mut(&mut self) -> &mut SQLiteGuestAllocators {
            &mut self.allocators
        }

        fn sqlite_statements(&self) -> &SQLiteStatements {
            &self.statements
        }

        fn sqlite_statements_mut(&mut self) -> &mut SQLiteStatements {
            &mut self.statements
        }

        fn sqlite_quota(&self) -> Arc<dyn SQLiteResourceQuota> {
            self.stats.clone()
        }
    }

    #[test]
    fn sqlite_quota_rejects_at_boundary_and_reuses_released_slots() -> Result<()> {
        let stats: Arc<dyn SQLiteResourceQuota> = Arc::new(SQLiteResourceStats::new(1, 1));
        let connection = SQLiteResourceLease::connection(Arc::clone(&stats))?;
        assert!(SQLiteResourceLease::connection(Arc::clone(&stats)).is_err());
        drop(connection);
        let connection = SQLiteResourceLease::connection(Arc::clone(&stats))?;

        let statement = SQLiteResourceLease::statement(Arc::clone(&stats))?;
        assert!(SQLiteResourceLease::statement(Arc::clone(&stats)).is_err());
        drop(statement);
        let statement = SQLiteResourceLease::statement(stats)?;
        drop(statement);
        drop(connection);
        Ok(())
    }

    #[test]
    fn closing_connection_defers_native_drop_and_quota_until_statement_finalize() -> Result<()> {
        let mut ctx = TestCtx::new(1, 1);
        let connection_id = ctx.open_memory_connection()?;
        let statement_id = prepare_statement(&mut ctx, connection_id, "SELECT 1")?;
        assert_eq!(ctx.stats.counts(), (1, 1));

        drop(ctx.connections.remove(connection_id));
        assert_eq!(ctx.stats.counts(), (1, 1));
        assert!(ctx.open_memory_connection().is_err());

        drop(ctx.statements.remove(statement_id));
        assert_eq!(ctx.stats.counts(), (0, 0));
        let replacement_id = ctx.open_memory_connection()?;
        drop(ctx.connections.remove(replacement_id));
        assert_eq!(ctx.stats.counts(), (0, 0));
        Ok(())
    }

    #[test]
    fn failed_statement_prepare_rolls_quota_back() -> Result<()> {
        let mut ctx = TestCtx::new(1, 1);
        let connection_id = ctx.open_memory_connection()?;

        assert!(prepare_statement(&mut ctx, connection_id, "not valid SQL").is_err());
        assert_eq!(ctx.stats.counts(), (1, 0));

        let statement_id = prepare_statement(&mut ctx, connection_id, "SELECT 1")?;
        assert_eq!(ctx.stats.counts(), (1, 1));
        drop(ctx.statements.remove(statement_id));
        drop(ctx.connections.remove(connection_id));
        assert_eq!(ctx.stats.counts(), (0, 0));
        Ok(())
    }
}
