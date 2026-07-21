/*!
The [`Message`] is a special variant of a [`Signal`](crate::Signal) that can be sent to
processes. The most common kind of Message is a [`DataMessage`], but there are also some special
kinds of messages, like the [`Message::LinkDied`], that is received if a linked process dies.
*/

use std::{
    any::Any,
    fmt::Debug,
    io::{Read, Write},
    sync::Arc,
};

use lunatic_networking_api::{
    NetworkHandleLease, NetworkHandleQuota, TcpConnection, TlsConnection,
};
use tokio::net::UdpSocket;

use crate::runtimes::wasmtime::WasmtimeCompiledModule;

pub type Resource = dyn Any + Send + Sync;

/// A network resource attached to a message together with the quota unit that
/// owns it while it is outside a process's network resource table.
pub struct MessageNetworkResource<T> {
    resource: T,
    lease: NetworkHandleLease,
}

impl<T> MessageNetworkResource<T> {
    pub fn new(resource: T, lease: NetworkHandleLease) -> Self {
        Self { resource, lease }
    }

    /// Moves the quota reservation to another process. A failed transfer
    /// leaves this message resource and its original reservation unchanged.
    pub fn transfer_to(&mut self, quota: Arc<dyn NetworkHandleQuota>) -> anyhow::Result<()> {
        self.lease.transfer_to(quota)
    }

    /// Returns the raw table resource while keeping its current quota unit
    /// reserved for the destination resource-table entry.
    pub fn into_table_resource(self) -> T {
        let Self { resource, lease } = self;
        lease.into_table_reservation();
        resource
    }
}

impl<T> Debug for MessageNetworkResource<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessageNetworkResource")
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

/// Can be sent between processes by being embedded into a  [`Signal::Message`][0]
///
/// A [`Message`] has 2 variants:
/// * Data - Regular message containing a tag, buffer and resources.
/// * LinkDied - A `LinkDied` signal that was turned into a message.
///
/// [0]: crate::Signal
#[derive(Debug)]
pub enum Message {
    Data(DataMessage),
    LinkDied(Option<i64>),
    ProcessDied(u64),
}

impl Message {
    pub fn tag(&self) -> Option<i64> {
        match self {
            Message::Data(message) => message.tag,
            Message::LinkDied(tag) => *tag,
            Message::ProcessDied(_) => None,
        }
    }

    pub fn process_id(&self) -> Option<u64> {
        match self {
            Message::Data(_) => None,
            Message::LinkDied(_) => None,
            Message::ProcessDied(process_id) => Some(*process_id),
        }
    }

    #[cfg(feature = "metrics")]
    pub fn write_metrics(&self) {
        match self {
            Message::Data(message) => message.write_metrics(),
            Message::LinkDied(_) => {
                metrics::increment_counter!("lunatic.process.messages.link_died.count");
            }
            Message::ProcessDied(_) => {}
        }
    }
}

/// A variant of a [`Message`] that has a buffer of data and resources attached to it.
///
/// It implements the [`Read`](std::io::Read) and [`Write`](std::io::Write) traits.
#[derive(Debug, Default)]
pub struct DataMessage {
    // TODO: Only the Node implementation depends on these fields being public.
    pub tag: Option<i64>,
    pub read_ptr: usize,
    pub buffer: Vec<u8>,
    pub resources: Vec<Option<Arc<Resource>>>,
}

impl DataMessage {
    /// Create a new message.
    pub fn new(tag: Option<i64>, buffer_capacity: usize) -> Self {
        Self {
            tag,
            read_ptr: 0,
            buffer: Vec::with_capacity(buffer_capacity),
            resources: Vec::new(),
        }
    }

    /// Create a new message from a vec.
    pub fn new_from_vec(tag: Option<i64>, buffer: Vec<u8>) -> Self {
        Self {
            tag,
            read_ptr: 0,
            buffer,
            resources: Vec::new(),
        }
    }

    /// Adds a resource to the message and returns the index of it inside of the message.
    ///
    /// The resource is `Any` and is downcasted when accessing later.
    pub fn add_resource(&mut self, resource: Arc<Resource>) -> usize {
        if self.resources.len() == self.resources.capacity() {
            // Resource-table capacity is retained with the message while it
            // waits outside the Wasm Store, so avoid geometric over-allocation
            // beyond the configured logical slot ceiling.
            self.resources.reserve_exact(1);
        }
        self.resources.push(Some(resource));
        self.resources.len() - 1
    }

    /// Adds an exclusively owned network resource to the message.
    pub fn add_network_resource<T>(&mut self, resource: MessageNetworkResource<T>) -> usize
    where
        T: Send + Sync + 'static,
    {
        self.add_resource(Arc::new(resource))
    }

    /// Takes a module from the message, but preserves the indexes of all others.
    ///
    /// If the index is out of bound or the resource is not a module the function will return
    /// None.
    pub fn take_module<T: 'static>(
        &mut self,
        index: usize,
    ) -> Option<Arc<WasmtimeCompiledModule<T>>> {
        self.take_downcast(index)
    }

    /// Takes a TCP stream from the message, but preserves the indexes of all others.
    ///
    /// If the index is out of bounds or the resource is not a TCP stream, the function returns
    /// None. This compatibility accessor only extracts legacy unleased values;
    /// host-created messages use [`Self::take_leased_tcp_stream`] so quota
    /// ownership can be transferred safely.
    pub fn take_tcp_stream(&mut self, index: usize) -> Option<Arc<TcpConnection>> {
        self.take_downcast(index)
    }

    /// Takes a TCP stream carrying a transferable quota lease.
    pub fn take_leased_tcp_stream(
        &mut self,
        index: usize,
    ) -> Option<MessageNetworkResource<Arc<TcpConnection>>> {
        self.take_network_resource(index)
    }

    /// Takes a UDP Socket from the message, but preserves the indexes of all others.
    ///
    /// If the index is out of bounds or the resource is not a UDP socket, the function returns
    /// None. This compatibility accessor only extracts legacy unleased values;
    /// host-created messages use [`Self::take_leased_udp_socket`] so quota
    /// ownership can be transferred safely.
    pub fn take_udp_socket(&mut self, index: usize) -> Option<Arc<UdpSocket>> {
        self.take_downcast(index)
    }

    /// Takes a UDP socket carrying a transferable quota lease.
    pub fn take_leased_udp_socket(
        &mut self,
        index: usize,
    ) -> Option<MessageNetworkResource<Arc<UdpSocket>>> {
        self.take_network_resource(index)
    }

    /// Takes a TLS stream from the message, but preserves the indexes of all others.
    ///
    /// If the index is out of bounds or the resource is not a TLS stream, the function returns
    /// None. This compatibility accessor only extracts legacy unleased values;
    /// host-created messages use [`Self::take_leased_tls_stream`] so quota
    /// ownership can be transferred safely.
    pub fn take_tls_stream(&mut self, index: usize) -> Option<Arc<TlsConnection>> {
        self.take_downcast(index)
    }

    /// Takes a TLS stream carrying a transferable quota lease.
    pub fn take_leased_tls_stream(
        &mut self,
        index: usize,
    ) -> Option<MessageNetworkResource<Arc<TlsConnection>>> {
        self.take_network_resource(index)
    }

    /// Restores a network resource to the slot from which it was taken.
    ///
    /// This is used when destination quota admission fails. Returning `Err`
    /// gives ownership back to the caller, whose drop path then releases the
    /// still-active lease instead of leaking it.
    pub fn restore_network_resource<T>(
        &mut self,
        index: usize,
        resource: MessageNetworkResource<T>,
    ) -> Result<(), MessageNetworkResource<T>>
    where
        T: Send + Sync + 'static,
    {
        let Some(slot) = self.resources.get_mut(index) else {
            return Err(resource);
        };
        if slot.is_some() {
            return Err(resource);
        }
        *slot = Some(Arc::new(resource));
        Ok(())
    }

    /// Moves read pointer to index.
    pub fn seek(&mut self, index: usize) {
        self.read_ptr = index;
    }

    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    #[cfg(feature = "metrics")]
    pub fn write_metrics(&self) {
        metrics::increment_counter!("lunatic.process.messages.data.count");
        metrics::histogram!(
            "lunatic.process.messages.data.resources.count",
            self.resources.len() as f64
        );
        metrics::histogram!("lunatic.process.messages.data.size", self.size() as f64);
    }

    fn take_downcast<T: Send + Sync + 'static>(&mut self, index: usize) -> Option<Arc<T>> {
        let resource = self.resources.get_mut(index);
        match resource {
            Some(resource_ref) => {
                let resource_any = std::mem::take(resource_ref).map(|resource| resource.downcast());
                match resource_any {
                    Some(Ok(resource)) => Some(resource),
                    Some(Err(resource)) => {
                        *resource_ref = Some(resource);
                        None
                    }
                    None => None,
                }
            }
            None => None,
        }
    }

    fn take_network_resource<T>(&mut self, index: usize) -> Option<MessageNetworkResource<T>>
    where
        T: Send + Sync + 'static,
    {
        let resource = self.take_downcast::<MessageNetworkResource<T>>(index)?;
        match Arc::try_unwrap(resource) {
            Ok(resource) => Some(resource),
            Err(resource) => {
                // Network message resources are move-only. If an out-of-tree
                // producer cloned the public `Arc`, preserve the slot and fail
                // closed rather than duplicating one quota lease.
                self.resources[index] = Some(resource);
                None
            }
        }
    }
}

impl Write for DataMessage {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for DataMessage {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let slice = if let Some(slice) = self.buffer.get(self.read_ptr..) {
            slice
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Reading outside message buffer",
            ));
        };
        let bytes = buf.write(slice)?;
        self.read_ptr += bytes;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use anyhow::anyhow;
    use lunatic_networking_api::{NetworkHandleLease, NetworkHandleQuota};

    use crate::{state::mailboxes_with_limits, Signal};

    use super::{DataMessage, Message, MessageNetworkResource};

    #[derive(Debug)]
    struct TestQuota {
        max: usize,
        current: AtomicUsize,
    }

    impl TestQuota {
        fn new(max: usize) -> Arc<Self> {
            Arc::new(Self {
                max,
                current: AtomicUsize::new(0),
            })
        }

        fn current(&self) -> usize {
            self.current.load(Ordering::SeqCst)
        }
    }

    impl NetworkHandleQuota for TestQuota {
        fn reserve(&self) -> anyhow::Result<()> {
            self.current
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    (current < self.max).then_some(current + 1)
                })
                .map(|_| ())
                .map_err(|_| anyhow!("network quota reached"))
        }

        fn release(&self) -> anyhow::Result<()> {
            self.current
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .map(|_| ())
                .map_err(|_| anyhow!("network quota underflow"))
        }
    }

    fn leased_resource(quota: &Arc<TestQuota>, value: u8) -> MessageNetworkResource<u8> {
        let owner: Arc<dyn NetworkHandleQuota> = quota.clone();
        MessageNetworkResource::new(value, NetworkHandleLease::reserve_new(owner).unwrap())
    }

    #[test]
    fn message_resource_keeps_sender_quota_and_drop_releases_it() {
        let quota = TestQuota::new(1);
        let mut message = DataMessage::new(None, 0);
        message.add_network_resource(leased_resource(&quota, 7));

        assert_eq!(quota.current(), 1);
        assert!(quota.reserve().is_err(), "message must still occupy quota");

        drop(message);
        assert_eq!(quota.current(), 0);
        quota.reserve().unwrap();
        quota.release().unwrap();
    }

    #[test]
    fn wrong_take_preserves_resource_and_sender_quota() {
        let quota = TestQuota::new(1);
        let mut message = DataMessage::new(None, 0);
        let index = message.add_network_resource(leased_resource(&quota, 7));

        assert!(message.take_network_resource::<u16>(index).is_none());
        assert!(message.resources[index].is_some());
        assert_eq!(quota.current(), 1);

        drop(message);
        assert_eq!(quota.current(), 0);
    }

    #[test]
    fn failed_send_returns_message_with_its_sender_lease() {
        let quota = TestQuota::new(1);
        let mut message = DataMessage::new(None, 0);
        message.add_network_resource(leased_resource(&quota, 7));
        let ((sender, receiver), _mailbox) = mailboxes_with_limits(1, 1, 0, 1);
        drop(receiver);

        let error = sender
            .send(Signal::Message(Message::Data(message)))
            .unwrap_err();
        assert_eq!(quota.current(), 1);

        let Signal::Message(returned) = error.into_signal() else {
            panic!("failed message send must return the original message")
        };
        drop(returned);
        assert_eq!(quota.current(), 0);
    }

    #[test]
    fn failed_destination_admission_can_restore_original_lease() {
        let source = TestQuota::new(1);
        let destination = TestQuota::new(0);
        let mut message = DataMessage::new(None, 0);
        let index = message.add_network_resource(leased_resource(&source, 7));
        let mut resource = message.take_network_resource::<u8>(index).unwrap();

        let destination_owner: Arc<dyn NetworkHandleQuota> = destination.clone();
        assert!(resource.transfer_to(destination_owner).is_err());
        message.restore_network_resource(index, resource).unwrap();

        assert_eq!(source.current(), 1);
        assert_eq!(destination.current(), 0);
        assert!(message.resources[index].is_some());
        drop(message);
        assert_eq!(source.current(), 0);
    }

    #[test]
    fn successful_take_moves_quota_exactly_once() {
        let source = TestQuota::new(1);
        let destination = TestQuota::new(1);
        let mut message = DataMessage::new(None, 0);
        let index = message.add_network_resource(leased_resource(&source, 7));
        let mut resource = message.take_network_resource::<u8>(index).unwrap();

        let destination_owner: Arc<dyn NetworkHandleQuota> = destination.clone();
        resource.transfer_to(destination_owner).unwrap();
        assert_eq!(source.current(), 0);
        assert_eq!(destination.current(), 1);

        assert_eq!(resource.into_table_resource(), 7);
        assert_eq!(destination.current(), 1);
        destination.release().unwrap();
        assert_eq!(destination.current(), 0);
    }

    #[test]
    fn same_owner_take_at_limit_keeps_one_reservation() {
        let quota = TestQuota::new(1);
        let mut message = DataMessage::new(None, 0);
        let index = message.add_network_resource(leased_resource(&quota, 7));
        let mut resource = message.take_network_resource::<u8>(index).unwrap();

        let same_owner: Arc<dyn NetworkHandleQuota> = quota.clone();
        resource.transfer_to(same_owner).unwrap();
        assert_eq!(quota.current(), 1);

        assert_eq!(resource.into_table_resource(), 7);
        assert_eq!(quota.current(), 1);
        quota.release().unwrap();
    }
}
