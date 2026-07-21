use std::{
    convert::TryInto,
    future::Future,
    io::{Read, Write},
};

use anyhow::{anyhow, Result};
use lunatic_common_api::{get_memory, IntoTrap, LinkerAsyncExt};
use lunatic_networking_api::{NetworkHandleLease, NetworkingCtx};
use lunatic_process_api::ProcessCtx;
use tokio::time::{timeout, Duration};
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

use lunatic_process::{
    config::ProcessConfig,
    message::{DataMessage, Message, MessageNetworkResource},
    state::{ProcessState, SignalSendError, SignalSendErrorKind},
    Signal,
};

// Register the mailbox APIs to the linker
pub fn register<T: ProcessState + ProcessCtx<T> + NetworkingCtx + Send + 'static>(
    linker: &mut Linker<T>,
) -> Result<()> {
    linker.func_wrap(
        "lunatic::message",
        "create_data",
        |caller: Caller<T>, tag: i64, buffer_capacity: u64| {
            create_data(caller, tag, buffer_capacity).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::message",
        "write_data",
        |caller: Caller<T>, data_ptr: u32, data_len: u32| {
            write_data(caller, data_ptr, data_len).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::message",
        "read_data",
        |caller: Caller<T>, data_ptr: u32, data_len: u32| {
            read_data(caller, data_ptr, data_len).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::message",
        "seek_data",
        |caller: Caller<T>, index: u64| seek_data(caller, index).to_wasmtime_result(),
    )?;
    linker.func_wrap("lunatic::message", "get_tag", |caller: Caller<T>| {
        get_tag(caller).to_wasmtime_result()
    })?;
    linker.func_wrap("lunatic::message", "get_process_id", |caller: Caller<T>| {
        get_process_id(caller).to_wasmtime_result()
    })?;
    linker.func_wrap("lunatic::message", "data_size", |caller: Caller<T>| {
        data_size(caller).to_wasmtime_result()
    })?;
    linker.func_wrap(
        "lunatic::message",
        "push_module",
        |caller: Caller<T>, module_id: u64| push_module(caller, module_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "take_module",
        |caller: Caller<T>, index: u64| take_module(caller, index).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "push_tcp_stream",
        |caller: Caller<T>, stream_id: u64| push_tcp_stream(caller, stream_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "take_tcp_stream",
        |caller: Caller<T>, index: u64| take_tcp_stream(caller, index).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "push_tls_stream",
        |caller: Caller<T>, stream_id: u64| push_tls_stream(caller, stream_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "take_tls_stream",
        |caller: Caller<T>, index: u64| take_tls_stream(caller, index).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "send",
        |caller: Caller<T>, process_id: u64| send(caller, process_id).to_wasmtime_result(),
    )?;
    linker.func_wrap3_async(
        "lunatic::message",
        "send_receive_skip_search",
        send_receive_skip_search,
    )?;
    linker.func_wrap3_async("lunatic::message", "receive", receive)?;
    linker.func_wrap(
        "lunatic::message",
        "push_udp_socket",
        |caller: Caller<T>, socket_id: u64| push_udp_socket(caller, socket_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::message",
        "take_udp_socket",
        |caller: Caller<T>, index: u64| take_udp_socket(caller, index).to_wasmtime_result(),
    )?;

    Ok(())
}

// There are two kinds of messages a lunatic process can receive:
//
// 1. **Data message** that contains a buffer of raw `u8` data and host side resources.
// 2. **LinkDied message**, representing a `LinkDied` signal that was turned into a message. The
//    process can control if when a link dies the process should die too, or just receive a
//    `LinkDied` message notifying it about the link's death.
//
// All messages have a `tag` allowing for selective receives. If there are already messages in the
// receiving queue, they will be first searched for a specific tag and the first match returned.
// Tags are just `i64` values, and a value of 0 indicates no-tag, meaning that it matches all
// messages.
//
// # Data messages
//
// Data messages can be created from inside a process and sent to others.
//
// They consists of two parts:
// * A buffer of raw data
// * An collection of resources
//
// If resources are sent between processes, their ID changes. The resource ID can for example
// be already taken in the receiving process. So we need a way to communicate the new ID on the
// receiving end.
//
// When the `create_data(tag, capacity)` function is called an empty message is allocated and both
// parts (buffer and resources) can be modified before it's sent to another process. If a new
// resource is added to the message, the index inside of the message is returned. This information
// can be now serialized inside the raw data buffer in some way.
//
// E.g. Serializing a structure like this:
//
// struct A {
//     a: String,
//     b: Process,
//     c: i32,
//     d: TcpStream
// }
//
// can be done by creating a new data message with `create_data(tag, capacity)`. `capacity` can
// be used as a hint to the host to pre-reserve the right buffer size. After a message is created,
// all the resources can be added to it with `add_*`, in this case the fields `b` & `d`. The
// returned values will be the indexes inside the message.
//
// Now the struct can be serialized for example into something like this:
//
// ["Some string" | [resource 0] | i32 value | [resource 1] ]
//
// [resource 0] & [resource 1] are just encoded as 0 and 1 u64 values, representing their index
// in the message. Now the message can be sent to another process with `send`.
//
// An important limitation here is that messages can only be worked on one at a time. If we
// called `create_data` again before sending the message, the current buffer and resources
// would be dropped.
//
// On the receiving side, first the `receive(tag)` function must be called. If `tag` has a value
// different from 0, the function will only return messages that have the specific `tag`. Once
// a message is received, we can read from its buffer or extract resources from it.
//
// This can be a bit confusing, because resources are just IDs (u64 values) themselves. But we
// still need to serialize them into different u64 values. Resources are inherently bound to a
// process and you can't access another resource just by guessing an ID from another process.
// The process of sending them around needs to be explicit.
//
// This API was designed around the idea that most guest languages will use some serialization
// library and turning resources into indexes is a way of serializing. The same is true for
// deserializing them on the receiving side, when an index needs to be turned into an actual
// resource ID.

// Creates a new data message.
//
// This message is intended to be modified by other functions in this namespace. Once
// `lunatic::message::send` is called it will be sent to another process.
//
// Arguments:
// * tag - An identifier that can be used for selective receives. If value is 0, no tag is used.
// * buffer_capacity - A hint to the message to pre-allocate a large enough buffer for writes.
//
// Traps:
// * If `buffer_capacity` exceeds the process's configured message-size limit.
// * If the requested host-side buffer reservation fails.
fn create_data<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    tag: i64,
    buffer_capacity: u64,
) -> Result<()> {
    let max_message_size = caller.data().config().get_max_message_size();
    if buffer_capacity > max_message_size {
        return Err(anyhow!(
            "Message capacity {buffer_capacity} exceeds configured maximum {max_message_size}"
        ));
    }
    let buffer_capacity = usize::try_from(buffer_capacity)
        .map_err(|_| anyhow!("Message capacity exceeds platform maximum"))?;
    let tag = match tag {
        0 => None,
        tag => Some(tag),
    };
    let mut message = DataMessage::new(tag, 0);
    message
        .buffer
        .try_reserve_exact(buffer_capacity)
        .map_err(|error| anyhow!("Could not reserve message capacity: {error}"))?;
    caller
        .data_mut()
        .message_scratch_area()
        .replace(Message::Data(message));
    Ok(())
}

// Writes some data into the message buffer and returns how much data is written in bytes.
//
// Traps:
// * If any memory outside the guest heap space is referenced.
// * If it's called without a data message being inside of the scratch area.
// * If the resulting message exceeds the configured message-size limit.
fn write_data<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    data_ptr: u32,
    data_len: u32,
) -> Result<u32> {
    let max_message_size = caller.data().config().get_max_message_size();
    let start = data_ptr as usize;
    let end = start
        .checked_add(data_len as usize)
        .ok_or_else(|| anyhow!("Message source range overflow"))?;
    let memory = get_memory(&mut caller)?;
    // Validate guest memory before taking ownership from the scratch area, so
    // an invalid range cannot accidentally discard the in-progress message.
    memory
        .data(&caller)
        .get(start..end)
        .or_trap("lunatic::message::write_data")?;

    let mut message = caller
        .data_mut()
        .message_scratch_area()
        .take()
        .or_trap("lunatic::message::write_data")?;
    let validation = match &mut message {
        Message::Data(data) => match (data.size() as u64).checked_add(data_len as u64) {
            Some(requested_size) if requested_size > max_message_size => Err(anyhow!(
                "Message size {requested_size} exceeds configured maximum {max_message_size}"
            )),
            Some(_) => data
                .buffer
                // Avoid geometric over-allocation: the destination admission
                // limit accounts for retained host allocation, not only the
                // logical payload length.
                .try_reserve_exact(data_len as usize)
                .map_err(|error| anyhow!("Could not grow message buffer: {error}")),
            None => Err(anyhow!("Message size overflow")),
        },
        Message::LinkDied(_) => Err(anyhow!("Unexpected `Message::LinkDied` in scratch area")),
        Message::ProcessDied(_) => {
            Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    if let Err(error) = validation {
        caller.data_mut().message_scratch_area().replace(message);
        return Err(error);
    }

    let buffer = memory
        .data(&caller)
        .get(start..end)
        .expect("guest memory range was validated before taking the message");
    let write_result = match &mut message {
        Message::Data(data) => data.write(buffer),
        _ => unreachable!("message kind was validated before writing"),
    };
    caller.data_mut().message_scratch_area().replace(message);
    Ok(write_result.or_trap("lunatic::message::write_data")? as u32)
}

// Reads some data from the message buffer and returns how much data is read in bytes.
//
// Traps:
// * If any memory outside the guest heap space is referenced.
// * If it's called without a data message being inside of the scratch area.
fn read_data<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    data_ptr: u32,
    data_len: u32,
) -> Result<u32> {
    let memory = get_memory(&mut caller)?;
    let mut message = caller
        .data_mut()
        .message_scratch_area()
        .take()
        .or_trap("lunatic::message::read_data")?;
    let buffer = memory
        .data_mut(&mut caller)
        .get_mut(data_ptr as usize..(data_ptr as usize + data_len as usize))
        .or_trap("lunatic::message::read_data")?;
    let bytes = match &mut message {
        Message::Data(data) => data.read(buffer).or_trap("lunatic::message::read_data")?,
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    // Put message back after reading from it.
    caller.data_mut().message_scratch_area().replace(message);

    Ok(bytes as u32)
}

// Moves reading head of the internal message buffer. It's useful if you wish to read the a bit
// of a message, decide that someone else will handle it, `seek_data(0)` to reset the read
// position for the new receiver and `send` it to another process.
//
// Traps:
// * If it's called without a data message being inside of the scratch area.
fn seek_data<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>, index: u64) -> Result<()> {
    let mut message = caller
        .data_mut()
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::seek_data")?;
    match &mut message {
        Message::Data(data) => data.seek(index as usize),
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    Ok(())
}

// Returns the message tag or 0 if no tag was set.
//
// Traps:
// * If it's called without a message being inside of the scratch area.
fn get_tag<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>) -> Result<i64> {
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_ref()
        .or_trap("lunatic::message::get_tag")?;
    Ok(message.tag().unwrap_or(0))
}

// Returns the process id if the message is a process died signal, or 0 if any other message type.
//
// Traps:
// * If it's called without a message being inside of the scratch area.
fn get_process_id<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>) -> Result<u64> {
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_ref()
        .or_trap("lunatic::message::get_process_id")?;
    Ok(message.process_id().unwrap_or(0))
}

// Returns the size in bytes of the message buffer.
//
// Traps:
// * If it's called without a data message being inside of the scratch area.
fn data_size<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>) -> Result<u64> {
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_ref()
        .or_trap("lunatic::message::data_size")?;
    let bytes = match message {
        Message::Data(data) => data.size(),
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };

    Ok(bytes as u64)
}

// Adds a module resource to the message that is currently in the scratch area and returns
// the new location of it.
//
// Traps:
// * If module ID doesn't exist
// * If no data message is in the scratch area.
// * If the configured per-message resource limit has been reached.
fn ensure_message_resource_capacity<T: ProcessState + ProcessCtx<T>>(
    caller: &mut Caller<T>,
) -> Result<()> {
    let max_resources = caller.data().config().get_max_message_resources() as usize;
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::push_resource")?;
    match message {
        Message::Data(data) => reserve_message_resource_slot(data, max_resources),
        Message::LinkDied(_) => Err(anyhow!("Unexpected `Message::LinkDied` in scratch area")),
        Message::ProcessDied(_) => {
            Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    }
}

fn reserve_message_resource_slot(data: &mut DataMessage, max_resources: usize) -> Result<()> {
    if data.resources.len() >= max_resources {
        return Err(anyhow!("Message resource limit ({max_resources}) reached"));
    }
    data.resources
        .try_reserve_exact(1)
        .map_err(|error| anyhow!("Could not grow message resource table: {error}"))
}

fn ensure_data_message<T: ProcessState + ProcessCtx<T>>(caller: &mut Caller<T>) -> Result<()> {
    match caller
        .data_mut()
        .message_scratch_area()
        .as_ref()
        .or_trap("lunatic::message::resource")?
    {
        Message::Data(_) => Ok(()),
        Message::LinkDied(_) => Err(anyhow!("Unexpected `Message::LinkDied` in scratch area")),
        Message::ProcessDied(_) => {
            Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    }
}

fn push_module<T: ProcessState + ProcessCtx<T> + NetworkingCtx + 'static>(
    mut caller: Caller<T>,
    module_id: u64,
) -> Result<u64> {
    ensure_message_resource_capacity(&mut caller)?;
    let module = caller
        .data()
        .module_resources()
        .get(module_id)
        .or_trap("lunatic::message::push_module")?
        .clone();
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::push_module")?;
    let index = match message {
        Message::Data(data) => data.add_resource(module) as u64,
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    Ok(index)
}

// Takes the module from the message that is currently in the scratch area by index, puts
// it into the process' resources and returns the resource ID.
//
// Traps:
// * If index ID doesn't exist or matches the wrong resource (not a module).
// * If no data message is in the scratch area.
fn take_module<T: ProcessState + ProcessCtx<T> + NetworkingCtx + 'static>(
    mut caller: Caller<T>,
    index: u64,
) -> Result<u64> {
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::take_module")?;
    let module = match message {
        Message::Data(data) => data
            .take_module(index as usize)
            .or_trap("lunatic::message::take_module")?,
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    Ok(caller.data_mut().module_resources_mut().add(module))
}

// Adds a tcp stream resource to the message that is currently in the scratch area and returns
// the new location of it. This will remove the tcp stream from  the current process' resources.
//
// Traps:
// * If TCP stream ID doesn't exist
// * If no data message is in the scratch area.
// * If the configured per-message resource limit has been reached.
fn push_tcp_stream<T: ProcessState + ProcessCtx<T> + NetworkingCtx>(
    mut caller: Caller<T>,
    stream_id: u64,
) -> Result<u64> {
    ensure_message_resource_capacity(&mut caller)?;
    caller
        .data()
        .tcp_stream_resources()
        .get(stream_id)
        .or_trap("lunatic::message::push_tcp_stream")?;
    let quota = caller.data().network_handle_quota().ok_or_else(|| {
        anyhow!("lunatic::message::push_tcp_stream: transferable network quota unavailable")
    })?;
    let stream = caller
        .data_mut()
        .tcp_stream_resources_mut()
        .remove(stream_id)
        .expect("validated TCP stream must remain in the resource table");
    let stream = MessageNetworkResource::new(stream, NetworkHandleLease::from_existing(quota));
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::push_tcp_stream")?;
    let index = match message {
        Message::Data(data) => data.add_network_resource(stream) as u64,
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    Ok(index)
}

// Takes the tcp stream from the message that is currently in the scratch area by index, puts
// it into the process' resources and returns the resource ID.
//
// Traps:
// * If index ID doesn't exist or matches the wrong resource (not a tcp stream).
// * If no data message is in the scratch area.
fn take_tcp_stream<T: ProcessState + ProcessCtx<T> + NetworkingCtx>(
    mut caller: Caller<T>,
    index: u64,
) -> Result<u64> {
    ensure_data_message(&mut caller)?;
    let quota = caller.data().network_handle_quota().ok_or_else(|| {
        anyhow!("lunatic::message::take_tcp_stream: transferable network quota unavailable")
    })?;
    let mut tcp_stream =
        match caller
            .data_mut()
            .message_scratch_area()
            .as_mut()
            .and_then(|message| match message {
                Message::Data(data) => data.take_leased_tcp_stream(index as usize),
                _ => None,
            }) {
            Some(stream) => stream,
            None => {
                return Err(anyhow!(
                "lunatic::message::take_tcp_stream: resource doesn't exist or has the wrong type"
            ));
            }
        };
    if let Err(error) = tcp_stream.transfer_to(quota) {
        let message = caller
            .data_mut()
            .message_scratch_area()
            .as_mut()
            .expect("data message was validated before taking a TCP stream");
        let Message::Data(data) = message else {
            unreachable!("data message was validated before taking a TCP stream")
        };
        data.restore_network_resource(index as usize, tcp_stream)
            .expect("a failed TCP transfer must restore its vacated message slot");
        return Err(error);
    }
    Ok(caller
        .data_mut()
        .tcp_stream_resources_mut()
        .add(tcp_stream.into_table_resource()))
}

// move tls stream

// Adds a tls stream resource to the message that is currently in the scratch area and returns
// the new location of it. This will remove the tls stream from  the current process' resources.
//
// Traps:
// * If TLS stream ID doesn't exist
// * If no data message is in the scratch area.
// * If the configured per-message resource limit has been reached.
fn push_tls_stream<T: ProcessState + ProcessCtx<T> + NetworkingCtx>(
    mut caller: Caller<T>,
    stream_id: u64,
) -> Result<u64> {
    ensure_message_resource_capacity(&mut caller)?;
    caller
        .data()
        .tls_stream_resources()
        .get(stream_id)
        .or_trap("lunatic::message::push_tls_stream")?;
    let quota = caller.data().network_handle_quota().ok_or_else(|| {
        anyhow!("lunatic::message::push_tls_stream: transferable network quota unavailable")
    })?;
    let resources = caller.data_mut().tls_stream_resources_mut();
    let stream = resources
        .remove(stream_id)
        .expect("validated TLS stream must remain in the resource table");
    let stream = MessageNetworkResource::new(stream, NetworkHandleLease::from_existing(quota));
    let message = caller
        .data_mut()
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::push_tls_stream")?;
    let index = match message {
        Message::Data(data) => data.add_network_resource(stream) as u64,
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    Ok(index)
}

// Takes the tls stream from the message that is currently in the scratch area by index, puts
// it into the process' resources and returns the resource ID.
//
// Traps:
// * If index ID doesn't exist or matches the wrong resource (not a tls stream).
// * If no data message is in the scratch area.
fn take_tls_stream<T: ProcessState + ProcessCtx<T> + NetworkingCtx>(
    mut caller: Caller<T>,
    index: u64,
) -> Result<u64> {
    ensure_data_message(&mut caller)?;
    let quota = caller.data().network_handle_quota().ok_or_else(|| {
        anyhow!("lunatic::message::take_tls_stream: transferable network quota unavailable")
    })?;
    let mut tls_stream =
        match caller
            .data_mut()
            .message_scratch_area()
            .as_mut()
            .and_then(|message| match message {
                Message::Data(data) => data.take_leased_tls_stream(index as usize),
                _ => None,
            }) {
            Some(stream) => stream,
            None => {
                return Err(anyhow!(
                "lunatic::message::take_tls_stream: resource doesn't exist or has the wrong type"
            ));
            }
        };
    if let Err(error) = tls_stream.transfer_to(quota) {
        let message = caller
            .data_mut()
            .message_scratch_area()
            .as_mut()
            .expect("data message was validated before taking a TLS stream");
        let Message::Data(data) = message else {
            unreachable!("data message was validated before taking a TLS stream")
        };
        data.restore_network_resource(index as usize, tls_stream)
            .expect("a failed TLS transfer must restore its vacated message slot");
        return Err(error);
    }
    Ok(caller
        .data_mut()
        .tls_stream_resources_mut()
        .add(tls_stream.into_table_resource()))
}

const SEND_QUEUED: u32 = 0;
const SEND_CLOSED_OR_MISSING: u32 = 1;
const SEND_BACKPRESSURE: u32 = 2;

fn restore_failed_message<T: ProcessState + ProcessCtx<T>>(
    caller: &mut Caller<T>,
    error: SignalSendError,
) -> u32 {
    let status = match error.kind() {
        SignalSendErrorKind::Closed => SEND_CLOSED_OR_MISSING,
        SignalSendErrorKind::MailboxFull
        | SignalSendErrorKind::QueueFull
        | SignalSendErrorKind::MessageTooLarge
        | SignalSendErrorKind::TooManyMessageResources => SEND_BACKPRESSURE,
    };
    let Signal::Message(message) = error.into_signal() else {
        unreachable!("sending a message must return the original message on failure")
    };
    caller.data_mut().message_scratch_area().replace(message);
    status
}

fn try_send_message<T: ProcessState + ProcessCtx<T>>(
    caller: &mut Caller<T>,
    process_id: u64,
    message: Message,
) -> u32 {
    let Some(process) = caller.data().environment().get_process(process_id) else {
        caller.data_mut().message_scratch_area().replace(message);
        return SEND_CLOSED_OR_MISSING;
    };

    match process.send(Signal::Message(message)) {
        Ok(()) => SEND_QUEUED,
        Err(error) => restore_failed_message(caller, error),
    }
}

// Sends the message to a process using bounded, non-blocking admission.
//
// Returns:
// * 0 if the message was queued.
// * 1 if the process doesn't exist or its signal receiver is closed.
// * 2 if the destination mailbox/signal queue is full or the destination rejects
//     the message's byte/resource size.
//
// On every non-zero result, the original message is restored to the sender's
// scratch area so the guest can retry, redirect, or discard it explicitly.
//
// Traps:
// * If it's called before creating the next message.
fn send<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>, process_id: u64) -> Result<u32> {
    let message = caller
        .data_mut()
        .message_scratch_area()
        .take()
        .or_trap("lunatic::message::send::no_message")?;

    Ok(try_send_message(&mut caller, process_id, message))
}

// Sends the message to a process and waits for a reply, but doesn't look through existing
// messages in the mailbox queue while waiting. This is an optimization that only makes sense
// with tagged messages. In a request/reply scenario we can tag the request message with an
// unique tag and just wait on it specifically.
//
// This operation needs to be an atomic host function, if we jumped back into the guest we could
// miss out on the incoming message before `receive` is called.
//
// If timeout is specified (value different from `u64::MAX`), the function will return on timeout
// expiration with value 9027.
//
// Returns:
// * 0    if message arrived.
// * 1    if the destination process doesn't exist or is closed.
// * 2    if the destination mailbox/signal queue is full or the destination rejects
//        the message's byte/resource size.
// * 9027 if call timed out.
//
// A send failure returns immediately without waiting and restores the original
// request message to the scratch area.
//
// Traps:
// * If it's called with wrong data in the scratch area.
fn send_receive_skip_search<T: ProcessState + ProcessCtx<T> + Send>(
    mut caller: Caller<T>,
    process_id: u64,
    wait_on_tag: i64,
    timeout_duration: u64,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let message = caller
            .data_mut()
            .message_scratch_area()
            .take()
            .or_trap("lunatic::message::send_receive_skip_search")?;

        let send_status = try_send_message(&mut caller, process_id, message);
        if send_status != SEND_QUEUED {
            return Ok(send_status);
        }

        let tags = [wait_on_tag];
        let pop_skip_search_tag = caller.data_mut().mailbox().pop_skip_search(Some(&tags));
        if let Ok(message) = match timeout_duration {
            // Without timeout
            u64::MAX => Ok(pop_skip_search_tag.await),
            // With timeout
            t => timeout(Duration::from_millis(t), pop_skip_search_tag).await,
        } {
            // Put the message into the scratch area
            caller.data_mut().message_scratch_area().replace(message);
            Ok(0)
        } else {
            Ok(9027)
        }
    })
}

// Takes the next message out of the queue or blocks until the next message is received if queue
// is empty.
//
// If **tag_len** is a value greater than 0 it will block until a message is received matching any
// of the supplied tags. **tag_ptr** points to an array containing i64 value encoded as little
// endian values.
//
// If timeout is specified (value different from `u64::MAX`), the function will return on timeout
// expiration with value 9027.
//
// Once the message is received, functions like `lunatic::message::read_data()` can be used to
// extract data out of it.
//
// Returns:
// * 0    if it's a data message.
// * 1    if it's a link died signal.
// * 2    if it's a process died signal.
// * 9027 if call timed out.
//
// Traps:
// * If **tag_ptr + (ciovec_array_len * 8) is outside the memory
fn receive<T: ProcessState + ProcessCtx<T> + Send>(
    mut caller: Caller<T>,
    tag_ptr: u32,
    tag_len: u32,
    timeout_duration: u64,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let tags = if tag_len > 0 {
            let memory = get_memory(&mut caller)?;
            let buffer = memory
                .data(&caller)
                .get(tag_ptr as usize..(tag_ptr + tag_len * 8) as usize)
                .or_trap("lunatic::message::receive")?;

            // Gether all tags
            let tags: Vec<i64> = buffer
                .chunks_exact(8)
                .map(|chunk| i64::from_le_bytes(chunk.try_into().expect("works")))
                .collect();
            Some(tags)
        } else {
            None
        };

        let pop = caller.data_mut().mailbox().pop(tags.as_deref());
        if let Ok(message) = match timeout_duration {
            // Without timeout
            u64::MAX => Ok(pop.await),
            // With timeout
            t => timeout(Duration::from_millis(t), pop).await,
        } {
            let result = match message {
                Message::Data(_) => 0,
                Message::LinkDied(_) => 1,
                Message::ProcessDied(_) => 2,
            };
            // Put the message into the scratch area
            caller.data_mut().message_scratch_area().replace(message);
            Ok(result)
        } else {
            Ok(9027)
        }
    })
}

// Adds a udp socket resource to the message that is currently in the scratch area and returns
// the new location of it. This will remove the socket from the current process' resources.
//
// Traps:
// * If UDP socket ID doesn't exist
// * If no data message is in the scratch area.
// * If the configured per-message resource limit has been reached.
fn push_udp_socket<T: ProcessState + ProcessCtx<T> + NetworkingCtx>(
    mut caller: Caller<T>,
    socket_id: u64,
) -> Result<u64> {
    ensure_message_resource_capacity(&mut caller)?;
    caller
        .data()
        .udp_resources()
        .get(socket_id)
        .or_trap("lunatic::message::push_udp_socket")?;
    let quota = caller.data().network_handle_quota().ok_or_else(|| {
        anyhow!("lunatic::message::push_udp_socket: transferable network quota unavailable")
    })?;
    let data = caller.data_mut();
    let socket = data
        .udp_resources_mut()
        .remove(socket_id)
        .expect("validated UDP socket must remain in the resource table");
    let socket = MessageNetworkResource::new(socket, NetworkHandleLease::from_existing(quota));
    let message = data
        .message_scratch_area()
        .as_mut()
        .or_trap("lunatic::message::push_udp_socket")?;
    let index = match message {
        Message::Data(data) => data.add_network_resource(socket) as u64,
        Message::LinkDied(_) => {
            return Err(anyhow!("Unexpected `Message::LinkDied` in scratch area"))
        }
        Message::ProcessDied(_) => {
            return Err(anyhow!("Unexpected `Message::ProcessDied` in scratch area"))
        }
    };
    Ok(index)
}

// Takes the udp socket from the message that is currently in the scratch area by index, puts
// it into the process' resources and returns the resource ID.
//
// Traps:
// * If index ID doesn't exist or matches the wrong resource (not a udp socket).
// * If no data message is in the scratch area.
fn take_udp_socket<T: ProcessState + ProcessCtx<T> + NetworkingCtx>(
    mut caller: Caller<T>,
    index: u64,
) -> Result<u64> {
    ensure_data_message(&mut caller)?;
    let quota = caller.data().network_handle_quota().ok_or_else(|| {
        anyhow!("lunatic::message::take_udp_socket: transferable network quota unavailable")
    })?;
    let mut udp_socket =
        match caller
            .data_mut()
            .message_scratch_area()
            .as_mut()
            .and_then(|message| match message {
                Message::Data(data) => data.take_leased_udp_socket(index as usize),
                _ => None,
            }) {
            Some(socket) => socket,
            None => {
                return Err(anyhow!(
                "lunatic::message::take_udp_socket: resource doesn't exist or has the wrong type"
            ));
            }
        };
    if let Err(error) = udp_socket.transfer_to(quota) {
        let message = caller
            .data_mut()
            .message_scratch_area()
            .as_mut()
            .expect("data message was validated before taking a UDP socket");
        let Message::Data(data) = message else {
            unreachable!("data message was validated before taking a UDP socket")
        };
        data.restore_network_resource(index as usize, udp_socket)
            .expect("a failed UDP transfer must restore its vacated message slot");
        return Err(error);
    }
    Ok(caller
        .data_mut()
        .udp_resources_mut()
        .add(udp_socket.into_table_resource()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lunatic_process::message::{DataMessage, Resource};

    use super::reserve_message_resource_slot;

    #[test]
    fn non_power_of_two_resource_limit_does_not_overallocate_slots() {
        let mut message = DataMessage::new(None, 0);

        for value in 0_u8..5 {
            reserve_message_resource_slot(&mut message, 5).unwrap();
            let resource: Arc<Resource> = Arc::new(value);
            message.add_resource(resource);
        }

        assert_eq!(message.resources.len(), 5);
        assert_eq!(message.resources.capacity(), 5);
        assert!(reserve_message_resource_slot(&mut message, 5).is_err());
        assert_eq!(message.resources.capacity(), 5);
    }
}
