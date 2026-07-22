use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::{
    config::{DEFAULT_MAX_MESSAGE_RESOURCES, DEFAULT_MAX_MESSAGE_SIZE},
    message::Message,
};

/// Default maximum number of messages owned by a process mailbox.
///
/// A message that has been admitted to the signal queue but has not yet been
/// transferred into this mailbox also counts against this limit.
pub const DEFAULT_MESSAGE_MAILBOX_CAPACITY: usize = 1_024;

/// The `MessageMailbox` is a data structure holding all messages of a process.
///
/// If a `Signal` of type `Message` is received it will be taken from the Signal queue and put into
/// this structure. The order of messages is preserved. This struct also implements the [`Future`]
/// trait and `pop()` operations can be awaited on if the queue is empty.
///
/// ## Safety
///
/// This should be cancellation safe and can be used inside `tokio::select!` statements:
/// https://docs.rs/tokio/1.10.0/tokio/macro.select.html#cancellation-safety
#[derive(Clone)]
pub struct MessageMailbox {
    inner: Arc<Mutex<InnerMessageMailbox>>,
    admission: Arc<Semaphore>,
    capacity: usize,
    max_message_bytes: u64,
    max_message_resources: u32,
}

#[derive(Default)]
struct InnerMessageMailbox {
    waker: Option<Waker>,
    tags: Option<Vec<i64>>,
    // A message selected for a waiting receive plus its arrival position in
    // `messages`. If the receive is cancelled after being woken, restoring at
    // this position preserves FIFO relative to both older unmatched messages
    // and messages that arrived after it.
    found: Option<(usize, AdmittedMessage)>,
    messages: VecDeque<AdmittedMessage>,
}

struct AdmittedMessage {
    message: Message,
    // Releasing this permit is the single source of truth for freeing one
    // mailbox slot. It deliberately travels with a message while the message
    // moves between the signal queue, `found`, snapshots, and `messages`.
    _permit: MailboxPermit,
}

impl AdmittedMessage {
    fn new(message: Message, permit: MailboxPermit) -> Self {
        Self {
            message,
            _permit: permit,
        }
    }

    fn into_message(self) -> Message {
        self.message
    }
}

/// An owned reservation for one slot in a [`MessageMailbox`].
///
/// Permits are intentionally not cloneable. A bounded signal queue uses this
/// type to keep a future mailbox message admitted while it is staged as a
/// signal, then transfers it into the destination mailbox without a second
/// capacity check.
pub struct MailboxPermit {
    permit: OwnedSemaphorePermit,
}

impl MailboxPermit {
    fn belongs_to(&self, mailbox: &MessageMailbox) -> bool {
        Arc::ptr_eq(self.permit.semaphore(), &mailbox.admission)
    }
}

impl fmt::Debug for MailboxPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MailboxPermit { .. }")
    }
}

/// The reason a message could not be pushed into a mailbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MailboxPushErrorKind {
    /// The data-message buffer allocation exceeds the mailbox byte limit.
    MessageTooLarge,
    /// The data-message resource allocation exceeds the mailbox slot limit.
    TooManyMessageResources,
    /// The mailbox has no unreserved slots left.
    Full,
    /// The supplied permit belongs to a different mailbox.
    ForeignPermit,
}

/// An ownership-preserving mailbox push failure.
///
/// Use [`MailboxPushError::into_message`] to recover the original payload, or
/// [`MailboxPushError::into_parts`] when a foreign permit must also be
/// recovered.
pub enum MailboxPushError {
    MessageTooLarge {
        message: Message,
        permit: Option<MailboxPermit>,
        actual: u64,
        max: u64,
    },
    TooManyMessageResources {
        message: Message,
        permit: Option<MailboxPermit>,
        actual: u64,
        max: u32,
    },
    Full(Message),
    ForeignPermit {
        message: Message,
        permit: MailboxPermit,
    },
}

enum MessageLimitViolation {
    Bytes { actual: u64, max: u64 },
    Resources { actual: u64, max: u32 },
}

impl MailboxPushError {
    pub fn kind(&self) -> MailboxPushErrorKind {
        match self {
            Self::MessageTooLarge { .. } => MailboxPushErrorKind::MessageTooLarge,
            Self::TooManyMessageResources { .. } => MailboxPushErrorKind::TooManyMessageResources,
            Self::Full(_) => MailboxPushErrorKind::Full,
            Self::ForeignPermit { .. } => MailboxPushErrorKind::ForeignPermit,
        }
    }

    pub fn into_message(self) -> Message {
        match self {
            Self::MessageTooLarge { message, .. }
            | Self::TooManyMessageResources { message, .. }
            | Self::Full(message)
            | Self::ForeignPermit { message, .. } => message,
        }
    }

    pub fn into_parts(self) -> (Message, Option<MailboxPermit>) {
        match self {
            Self::MessageTooLarge {
                message, permit, ..
            }
            | Self::TooManyMessageResources {
                message, permit, ..
            } => (message, permit),
            Self::Full(message) => (message, None),
            Self::ForeignPermit { message, permit } => (message, Some(permit)),
        }
    }
}

impl fmt::Debug for MailboxPushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("MailboxPushError");
        debug.field("kind", &self.kind());
        match self {
            Self::MessageTooLarge { actual, max, .. } => {
                debug.field("actual", actual).field("max", max);
            }
            Self::TooManyMessageResources { actual, max, .. } => {
                debug.field("actual", actual).field("max", max);
            }
            Self::Full(_) | Self::ForeignPermit { .. } => {}
        }
        debug.finish_non_exhaustive()
    }
}

impl fmt::Display for MailboxPushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            MailboxPushErrorKind::MessageTooLarge => {
                formatter.write_str("message buffer exceeds mailbox byte limit")
            }
            MailboxPushErrorKind::TooManyMessageResources => {
                formatter.write_str("message resource table exceeds mailbox slot limit")
            }
            MailboxPushErrorKind::Full => formatter.write_str("message mailbox is full"),
            MailboxPushErrorKind::ForeignPermit => {
                formatter.write_str("mailbox permit belongs to another mailbox")
            }
        }
    }
}

impl Error for MailboxPushError {}

/// An ownership-preserving snapshot of all pending mailbox messages.
///
/// The snapshot retains every admission permit, so taking a snapshot cannot
/// temporarily open capacity for more messages. It can only be restored into
/// the mailbox (or one of its clones) from which it was captured.
pub struct MessageMailboxSnapshot {
    admission: Arc<Semaphore>,
    messages: VecDeque<AdmittedMessage>,
}

impl MessageMailboxSnapshot {
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

impl fmt::Debug for MessageMailboxSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageMailboxSnapshot")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

/// Returned when a snapshot is restored into a different mailbox.
pub struct MailboxRestoreError {
    snapshot: MessageMailboxSnapshot,
}

impl MailboxRestoreError {
    pub fn into_snapshot(self) -> MessageMailboxSnapshot {
        self.snapshot
    }
}

impl fmt::Debug for MailboxRestoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailboxRestoreError")
            .field("snapshot_len", &self.snapshot.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for MailboxRestoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("snapshot belongs to another message mailbox")
    }
}

impl Error for MailboxRestoreError {}

impl Default for MessageMailbox {
    fn default() -> Self {
        Self::new(DEFAULT_MESSAGE_MAILBOX_CAPACITY)
    }
}

impl MessageMailbox {
    /// Creates a mailbox with a finite message capacity.
    pub fn new(capacity: usize) -> Self {
        Self::with_limits(
            capacity,
            DEFAULT_MAX_MESSAGE_SIZE,
            DEFAULT_MAX_MESSAGE_RESOURCES,
        )
    }

    /// Creates a mailbox with explicit count and retained-allocation limits.
    pub fn with_limits(
        capacity: usize,
        max_message_bytes: u64,
        max_message_resources: u32,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(InnerMessageMailbox::default())),
            admission: Arc::new(Semaphore::new(capacity)),
            capacity,
            max_message_bytes,
            max_message_resources,
        }
    }

    /// Returns the configured maximum number of admitted messages.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of slots that have not been reserved yet.
    ///
    /// This accounts for messages staged in the signal queue as well as
    /// messages already visible in the mailbox.
    pub fn available_capacity(&self) -> usize {
        self.admission.available_permits()
    }

    fn limit_violation(&self, message: &Message) -> Option<MessageLimitViolation> {
        let Message::Data(message) = message else {
            return None;
        };
        let bytes = u64::try_from(message.buffer.capacity()).unwrap_or(u64::MAX);
        if bytes > self.max_message_bytes {
            return Some(MessageLimitViolation::Bytes {
                actual: bytes,
                max: self.max_message_bytes,
            });
        }
        let resources = u64::try_from(message.resources.capacity()).unwrap_or(u64::MAX);
        if resources > u64::from(self.max_message_resources) {
            return Some(MessageLimitViolation::Resources {
                actual: resources,
                max: self.max_message_resources,
            });
        }
        None
    }

    /// Reserves a slot for a message that will subsequently enter this
    /// mailbox. This is primarily used by the bounded signal ingress.
    pub fn try_reserve(&self) -> Result<MailboxPermit, TryAcquireError> {
        Arc::clone(&self.admission)
            .try_acquire_owned()
            .map(|permit| MailboxPermit { permit })
    }

    /// Return message in FIFO order from mailbox.
    ///
    /// If function is called with a `tags` value different from None, it will only return the first
    /// message matching any of the tags.
    ///
    /// If no message exist, blocks until a message is received.
    pub async fn pop(&self, tags: Option<&[i64]>) -> Message {
        // Mailbox lock must be released before .await
        {
            let mut mailbox = self.inner.lock().expect("only accessed by one process");

            // If a found message exists here, it means that the previous `.await` was canceled
            // after a `wake()` call. To not lose this message it should be put into the queue.
            if let Some((index, found)) = mailbox.found.take() {
                let index = index.min(mailbox.messages.len());
                mailbox.messages.insert(index, found);
            }

            // When looking for specific tags, loop through all messages to check for it
            if let Some(tags) = tags {
                let index = mailbox.messages.iter().position(|x| {
                    // Only consider messages that also have a tag.
                    if let Some(tag) = x.message.tag() {
                        tags.contains(&tag)
                    } else {
                        false
                    }
                });
                // If message matching tags is found, remove it.
                if let Some(index) = index {
                    return mailbox
                        .messages
                        .remove(index)
                        .expect("must exist")
                        .into_message();
                }
            } else {
                // If not looking for a specific tags try to pop the first message available.
                if let Some(message) = mailbox.messages.pop_front() {
                    return message.into_message();
                }
            }
            // Mark the tags to wait on.
            mailbox.tags = tags.map(|tags| tags.into());
        }
        self.await
    }

    /// Similar to `pop`, but will assume right away that no message with this tags exists.
    ///
    /// Sometimes we know that the message we are waiting on can't have a particular tags already in
    /// the queue, so we can save ourself a search through the queue. This is often the case in a
    /// request/response architecture where we sent the tags to the remote server but couldn't have
    /// gotten it back yet.
    ///
    /// ### Safety
    ///
    /// It may not be clear right away why it's safe to skip looking through the queue. If we are
    /// waiting on a reply, didn't we already send the message and couldn't it already have been
    /// received and pushed into our queue?
    ///
    /// The way processes work is that they run a bit of code, *stop*, look for new signals/messages
    /// before running more code. This stop can only happen if there is an `.await` point in the
    /// code. Sending signals/messages is not an async task and we don't need to `.await` on it.
    /// When using this function we need to make sure that sending a specific tag and waiting on it
    /// doesn't contain any `.await` calls in-between. This implementation detail can be hidden
    /// inside of atomic host function calls so that end users don't need to worry about it.
    pub async fn pop_skip_search(&self, tags: Option<&[i64]>) -> Message {
        // Mailbox lock must be released before .await
        {
            let mut mailbox = self.inner.lock().expect("only accessed by one process");

            // If a found message exists here, it means that the previous `.await` was canceled
            // after a `wake()` call. To not lose this message it should be put into the queue.
            if let Some((index, found)) = mailbox.found.take() {
                let index = index.min(mailbox.messages.len());
                mailbox.messages.insert(index, found);
            }

            // Mark the tags to wait on.
            mailbox.tags = tags.map(|tags| tags.into());
        }
        self.await
    }

    /// Pushes a message into the mailbox.
    ///
    /// If the message is being .awaited on, this call will immediately notify the waker that it's
    /// ready, otherwise it will push it at the end of the queue.
    pub fn push(&self, message: Message) -> Result<(), MailboxPushError> {
        match self.limit_violation(&message) {
            Some(MessageLimitViolation::Bytes { actual, max }) => {
                return Err(MailboxPushError::MessageTooLarge {
                    message,
                    permit: None,
                    actual,
                    max,
                });
            }
            Some(MessageLimitViolation::Resources { actual, max }) => {
                return Err(MailboxPushError::TooManyMessageResources {
                    message,
                    permit: None,
                    actual,
                    max,
                });
            }
            None => {}
        }
        let permit = match self.try_reserve() {
            Ok(permit) => permit,
            Err(_) => return Err(MailboxPushError::Full(message)),
        };
        self.push_with_permit(message, permit)
    }

    /// Pushes a message using a slot previously reserved from this mailbox.
    ///
    /// This transfers, rather than reacquires, admission across the bounded
    /// signal queue. A permit from another mailbox is rejected together with
    /// the original message and permit.
    pub fn push_with_permit(
        &self,
        message: Message,
        permit: MailboxPermit,
    ) -> Result<(), MailboxPushError> {
        if !permit.belongs_to(self) {
            return Err(MailboxPushError::ForeignPermit { message, permit });
        }

        match self.limit_violation(&message) {
            Some(MessageLimitViolation::Bytes { actual, max }) => {
                return Err(MailboxPushError::MessageTooLarge {
                    message,
                    permit: Some(permit),
                    actual,
                    max,
                });
            }
            Some(MessageLimitViolation::Resources { actual, max }) => {
                return Err(MailboxPushError::TooManyMessageResources {
                    message,
                    permit: Some(permit),
                    actual,
                    max,
                });
            }
            None => {}
        }

        let message = AdmittedMessage::new(message, permit);
        let mut mailbox = self.inner.lock().expect("only accessed by one process");
        // If waiting on a new message notify executor that it arrived.
        if let Some(waker) = mailbox.waker.take() {
            // If waiting on specific tags only notify if tags are matched, otherwise forward every message.
            // Note that because of the short-circuit rule in Rust it's safe to use `unwrap()` here.
            if mailbox.tags.is_none()
                || (message.message.tag().is_some()
                    && mailbox
                        .tags
                        .as_ref()
                        .unwrap()
                        .contains(&message.message.tag().unwrap()))
            {
                let restore_index = mailbox.messages.len();
                mailbox.found = Some((restore_index, message));
                waker.wake();
                return Ok(());
            } else {
                // Put the waker back if this is not the message we are looking for.
                mailbox.waker = Some(waker);
            }
        }
        // Otherwise put message into queue
        mailbox.messages.push_back(message);
        Ok(())
    }

    /// Returns the number of messages currently available
    pub fn len(&self) -> usize {
        let mailbox = self.inner.lock().expect("only accessed by one process");

        mailbox.messages.len() + usize::from(mailbox.found.is_some())
    }

    /// Returns true if the mailbox has no available messages
    pub fn is_empty(&self) -> bool {
        let mailbox = self.inner.lock().expect("only accessed by one process");

        mailbox.messages.is_empty() && mailbox.found.is_none()
    }

    /// Snapshot all pending messages for hot reload preservation
    ///
    /// Note: This captures a snapshot of messages at the current moment.
    /// Messages with resources (DataMessage) may not be fully cloneable,
    /// so we take ownership and will need to restore them properly.
    pub fn snapshot(&self) -> MessageMailboxSnapshot {
        let mut mailbox = self.inner.lock().expect("only accessed by one process");

        if let Some((index, found)) = mailbox.found.take() {
            let index = index.min(mailbox.messages.len());
            mailbox.messages.insert(index, found);
        }

        MessageMailboxSnapshot {
            admission: Arc::clone(&self.admission),
            messages: std::mem::take(&mut mailbox.messages),
        }
    }

    /// Restore messages from snapshot after hot reload
    pub fn restore(&self, snapshot: MessageMailboxSnapshot) -> Result<(), MailboxRestoreError> {
        if !Arc::ptr_eq(&self.admission, &snapshot.admission) {
            return Err(MailboxRestoreError { snapshot });
        }

        let mut mailbox = self.inner.lock().expect("only accessed by one process");
        if let Some((index, found)) = mailbox.found.take() {
            let index = index.min(mailbox.messages.len());
            mailbox.messages.insert(index, found);
        }
        let mut messages = snapshot.messages;
        // Messages accepted while the snapshot was owned are newer than all
        // snapshotted messages. Append them instead of overwriting them.
        messages.append(&mut mailbox.messages);
        mailbox.messages = messages;
        mailbox.found = None;
        Ok(())
    }
}

impl Future for &MessageMailbox {
    type Output = Message;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut mailbox = self.inner.lock().expect("only accessed by one process");
        if let Some((_, message)) = mailbox.found.take() {
            Poll::Ready(message.into_message())
        } else {
            mailbox.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{Arc, Mutex},
        task::{Context, Poll, Wake},
    };

    use crate::{message::DataMessage, DeathReason};

    use super::{MailboxPushErrorKind, Message, MessageMailbox};

    #[tokio::test]
    async fn no_tags_signal_message() {
        let mailbox = MessageMailbox::default();
        let message = Message::LinkDied(None);
        mailbox.push(message).unwrap();
        let result = mailbox.pop(None).await;
        match result {
            Message::LinkDied(None) => (),
            _ => panic!("Wrong message received"),
        }
    }

    #[tokio::test]
    async fn tag_signal_message() {
        let mailbox = MessageMailbox::default();
        let tag = 1337;
        let message = Message::LinkDied(Some(tag));
        mailbox.push(message).unwrap();
        let message = mailbox.pop(None).await;
        assert_eq!(message.tag(), Some(tag));
    }

    #[tokio::test]
    async fn selective_receive_tag_signal_message() {
        let mailbox = MessageMailbox::default();
        let tag1 = 1;
        let tag2 = 2;
        let tag3 = 3;
        let tag4 = 4;
        let tag5 = 5;
        mailbox.push(Message::LinkDied(Some(tag1))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag2))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag3))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag4))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag5))).unwrap();
        let message = mailbox.pop(Some(&[tag2])).await;
        assert_eq!(message.tag(), Some(tag2));
        let message = mailbox.pop(Some(&[tag1])).await;
        assert_eq!(message.tag(), Some(tag1));
        let message = mailbox.pop(Some(&[tag3])).await;
        assert_eq!(message.tag(), Some(tag3));
        // The only 2 left over are 4 & 5
        let message = mailbox.pop(None).await;
        assert_eq!(message.tag(), Some(tag4));
        let message = mailbox.pop(None).await;
        assert_eq!(message.tag(), Some(tag5));
    }

    #[tokio::test]
    async fn multiple_receive_tags_signal_message() {
        let mailbox = MessageMailbox::default();
        let tag1 = 1;
        let tag2 = 2;
        let tag3 = 3;
        let tag4 = 4;
        let tag5 = 5;
        mailbox.push(Message::LinkDied(Some(tag1))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag2))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag3))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag4))).unwrap();
        mailbox.push(Message::LinkDied(Some(tag5))).unwrap();
        let message = mailbox.pop(Some(&[tag2, tag1, tag3])).await;
        assert_eq!(message.tag(), Some(tag1));
        let message = mailbox.pop(Some(&[tag2, tag1, tag3])).await;
        assert_eq!(message.tag(), Some(tag2));
        let message = mailbox.pop(Some(&[tag2, tag1, tag3])).await;
        assert_eq!(message.tag(), Some(tag3));
        // The only 2 left over are 4 & 5
        let message = mailbox.pop(None).await;
        assert_eq!(message.tag(), Some(tag4));
        let message = mailbox.pop(None).await;
        assert_eq!(message.tag(), Some(tag5));
    }

    #[derive(Clone)]
    struct FlagWaker(Arc<Mutex<bool>>);
    impl Wake for FlagWaker {
        fn wake(self: Arc<Self>) {
            let mut called = self.0.lock().unwrap();
            *called = true;
        }
    }
    #[test]
    fn waiting_on_none_activates_waker() {
        let mailbox = MessageMailbox::default();
        // Sending a message with any tags to a mailbox that is "awaiting" a `None` tags should
        // trigger the waker and return the tags.
        let tags = Some(1337);
        // Manually poll future
        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker_ref = waker.clone();
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);
        // Request tags None
        let fut = mailbox.pop(None);
        let mut fut = Box::pin(fut);
        // First poll will block
        let result = fut.as_mut().poll(&mut context);
        assert!(result.is_pending());
        assert!(!*waker_ref.0.lock().unwrap());
        // Pushing a message to the mailbox will call the waker
        mailbox.push(Message::LinkDied(tags)).unwrap();
        assert!(*waker_ref.0.lock().unwrap());
        // Next poll will return the value
        let result = fut.as_mut().poll(&mut context);
        assert!(result.is_ready());
    }

    #[test]
    fn waiting_on_tag_after_none() {
        let mailbox = MessageMailbox::default();
        // "Awaiting" a specific tags and receiving a `None` message should not trigger the waker.
        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker_ref = waker.clone();
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);
        // Request tags 1337
        let fut = mailbox.pop(Some(&[1337]));
        let mut fut = Box::pin(fut);
        // First poll will block
        let result = fut.as_mut().poll(&mut context);
        assert!(result.is_pending());
        assert!(!*waker_ref.0.lock().unwrap());
        // Pushing a message with the `None` tags should not trigger the waker
        mailbox.push(Message::LinkDied(None)).unwrap();
        assert!(!*waker_ref.0.lock().unwrap());
        // Next poll will still not have the value with the tags 1337
        let result = fut.as_mut().poll(&mut context);
        assert!(result.is_pending());
        // Pushing another None in the meantime should not remove the waker
        mailbox.push(Message::LinkDied(None)).unwrap();
        // Pushing a message with tags 1337 should trigger the waker
        mailbox.push(Message::LinkDied(Some(1337))).unwrap();
        assert!(*waker_ref.0.lock().unwrap());
        // Next poll will have the message ready
        let result = fut.as_mut().poll(&mut context);
        assert!(result.is_ready());
    }

    #[test]
    fn cancellation_safety() {
        let mailbox = MessageMailbox::default();
        // Manually poll future
        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker_ref = waker.clone();
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);
        let fut = mailbox.pop(None);
        let mut fut = Box::pin(fut);
        // First poll will block the future
        let result = fut.as_mut().poll(&mut context);
        assert!(result.is_pending());
        assert!(!*waker_ref.0.lock().unwrap());
        // Pushing a message with the `None` tags should call the waker()
        mailbox.push(Message::LinkDied(None)).unwrap();
        assert!(*waker_ref.0.lock().unwrap());
        // Dropping the future will cancel it
        drop(fut);
        // Next poll will not have the value with the tags 1337
        let fut = mailbox.pop(Some(&[1337]));
        tokio::pin!(fut);
        let result = fut.poll(&mut context);
        assert!(result.is_pending());
        // But will have the value None in the mailbox
        let fut = mailbox.pop(None);
        tokio::pin!(fut);
        let result = fut.poll(&mut context);
        match result {
            Poll::Ready(Message::LinkDied(tags)) => assert_eq!(tags, None),
            _ => panic!("Unexpected message"),
        }
    }

    #[tokio::test]
    async fn canceled_no_tag_receive_preserves_fifo_with_later_messages() {
        let mailbox = MessageMailbox::default();
        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);

        let mut receive = Box::pin(mailbox.pop(None));
        assert!(receive.as_mut().poll(&mut context).is_pending());

        mailbox.push(Message::LinkDied(Some(1))).unwrap();
        mailbox.push(Message::LinkDied(Some(2))).unwrap();
        drop(receive);

        assert_eq!(mailbox.pop(None).await.tag(), Some(1));
        assert_eq!(mailbox.pop(None).await.tag(), Some(2));
    }

    #[tokio::test]
    async fn canceled_selective_receive_preserves_global_fifo() {
        let mailbox = MessageMailbox::default();
        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);

        mailbox.push(Message::LinkDied(Some(1))).unwrap();
        let mut receive = Box::pin(mailbox.pop(Some(&[2])));
        assert!(receive.as_mut().poll(&mut context).is_pending());

        mailbox.push(Message::LinkDied(Some(2))).unwrap();
        mailbox.push(Message::LinkDied(Some(3))).unwrap();
        drop(receive);

        assert_eq!(mailbox.pop(None).await.tag(), Some(1));
        assert_eq!(mailbox.pop(None).await.tag(), Some(2));
        assert_eq!(mailbox.pop(None).await.tag(), Some(3));
    }

    #[tokio::test]
    async fn full_mailbox_returns_original_message_and_pop_releases_capacity() {
        let mailbox = MessageMailbox::new(1);
        assert_eq!(mailbox.capacity(), 1);
        assert_eq!(mailbox.available_capacity(), 1);

        mailbox
            .push(Message::ProcessDied {
                process_id: 7,
                reason: DeathReason::Failure,
            })
            .unwrap();
        assert_eq!(mailbox.len(), 1);
        assert_eq!(mailbox.available_capacity(), 0);

        let error = mailbox
            .push(Message::ProcessDied {
                process_id: 99,
                reason: DeathReason::Normal,
            })
            .unwrap_err();
        assert_eq!(error.kind(), MailboxPushErrorKind::Full);
        match error.into_message() {
            Message::ProcessDied { process_id, reason } => {
                assert_eq!(process_id, 99);
                assert_eq!(reason, DeathReason::Normal);
            }
            _ => panic!("full push returned the wrong message"),
        }

        match mailbox.pop(None).await {
            Message::ProcessDied { process_id, reason } => {
                assert_eq!(process_id, 7);
                assert_eq!(reason, DeathReason::Failure);
            }
            _ => panic!("mailbox returned the wrong message"),
        }
        assert!(mailbox.is_empty());
        assert_eq!(mailbox.available_capacity(), 1);

        mailbox
            .push(Message::ProcessDied {
                process_id: 100,
                reason: DeathReason::NoProcess,
            })
            .unwrap();
        assert_eq!(mailbox.len(), 1);
    }

    #[test]
    fn found_message_counts_toward_length_and_capacity_until_consumed() {
        let mailbox = MessageMailbox::new(1);
        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);

        let mut receive = Box::pin(mailbox.pop(None));
        assert!(receive.as_mut().poll(&mut context).is_pending());

        mailbox.push(Message::LinkDied(Some(1))).unwrap();
        assert_eq!(mailbox.len(), 1);
        assert!(!mailbox.is_empty());
        assert_eq!(mailbox.available_capacity(), 0);
        let error = mailbox.push(Message::LinkDied(Some(2))).unwrap_err();
        assert_eq!(error.kind(), MailboxPushErrorKind::Full);

        drop(receive);
        let mut receive = Box::pin(mailbox.pop(None));
        match receive.as_mut().poll(&mut context) {
            Poll::Ready(Message::LinkDied(tag)) => assert_eq!(tag, Some(1)),
            _ => panic!("canceled receive lost its found message"),
        }
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn snapshot_preserves_permits_and_fifo_until_same_mailbox_restore() {
        let mailbox = MessageMailbox::new(2);
        mailbox.push(Message::LinkDied(Some(1))).unwrap();
        mailbox.push(Message::LinkDied(Some(2))).unwrap();

        let snapshot = mailbox.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(mailbox.is_empty());
        assert_eq!(mailbox.available_capacity(), 0);
        assert_eq!(
            mailbox.push(Message::LinkDied(Some(3))).unwrap_err().kind(),
            MailboxPushErrorKind::Full
        );

        mailbox.restore(snapshot).unwrap();
        assert_eq!(mailbox.len(), 2);
        assert_eq!(mailbox.pop(None).await.tag(), Some(1));
        assert_eq!(mailbox.pop(None).await.tag(), Some(2));
        assert_eq!(mailbox.available_capacity(), 2);
    }

    #[tokio::test]
    async fn restore_keeps_messages_accepted_after_a_partial_snapshot() {
        let mailbox = MessageMailbox::new(3);
        mailbox.push(Message::LinkDied(Some(1))).unwrap();

        let snapshot = mailbox.snapshot();
        mailbox.push(Message::LinkDied(Some(2))).unwrap();
        mailbox.push(Message::LinkDied(Some(3))).unwrap();
        assert_eq!(mailbox.available_capacity(), 0);

        mailbox.restore(snapshot).unwrap();
        assert_eq!(mailbox.len(), 3);
        assert_eq!(mailbox.pop(None).await.tag(), Some(1));
        assert_eq!(mailbox.pop(None).await.tag(), Some(2));
        assert_eq!(mailbox.pop(None).await.tag(), Some(3));
        assert_eq!(mailbox.available_capacity(), 3);
    }

    #[tokio::test]
    async fn restore_keeps_a_post_snapshot_message_selected_for_a_waiter() {
        let mailbox = MessageMailbox::new(2);
        mailbox.push(Message::LinkDied(Some(1))).unwrap();
        let snapshot = mailbox.snapshot();

        let waker = FlagWaker(Arc::new(Mutex::new(false)));
        let waker = &Arc::new(waker).into();
        let mut context = Context::from_waker(waker);
        let mut receive = Box::pin(mailbox.pop(None));
        assert!(receive.as_mut().poll(&mut context).is_pending());
        mailbox.push(Message::LinkDied(Some(2))).unwrap();

        mailbox.restore(snapshot).unwrap();
        drop(receive);
        assert_eq!(mailbox.pop(None).await.tag(), Some(1));
        assert_eq!(mailbox.pop(None).await.tag(), Some(2));
        assert_eq!(mailbox.available_capacity(), 2);
    }

    #[tokio::test]
    async fn foreign_snapshot_and_permit_are_rejected_without_losing_ownership() {
        let source = MessageMailbox::new(1);
        let destination = MessageMailbox::new(1);
        source
            .push(Message::ProcessDied {
                process_id: 41,
                reason: DeathReason::Failure,
            })
            .unwrap();

        let snapshot = source.snapshot();
        let error = destination.restore(snapshot).unwrap_err();
        assert!(destination.is_empty());
        assert_eq!(source.available_capacity(), 0);
        source.restore(error.into_snapshot()).unwrap();
        assert!(matches!(
            source.pop(None).await,
            Message::ProcessDied {
                process_id: 41,
                reason: DeathReason::Failure
            }
        ));

        let permit = source.try_reserve().unwrap();
        let error = destination
            .push_with_permit(
                Message::ProcessDied {
                    process_id: 42,
                    reason: DeathReason::Normal,
                },
                permit,
            )
            .unwrap_err();
        assert_eq!(error.kind(), MailboxPushErrorKind::ForeignPermit);
        let (message, permit) = error.into_parts();
        source.push_with_permit(message, permit.unwrap()).unwrap();
        assert!(matches!(
            source.pop(None).await,
            Message::ProcessDied {
                process_id: 42,
                reason: DeathReason::Normal
            }
        ));
    }

    #[test]
    fn direct_push_rejects_retained_buffer_allocation_without_consuming_capacity() {
        let mailbox = MessageMailbox::with_limits(1, 3, 1);
        let message = Message::Data(DataMessage::new(Some(7), 4));

        let error = mailbox.push(message).unwrap_err();
        assert_eq!(error.kind(), MailboxPushErrorKind::MessageTooLarge);
        assert_eq!(mailbox.available_capacity(), 1);
        match error.into_message() {
            Message::Data(message) => {
                assert_eq!(message.tag, Some(7));
                assert!(message.buffer.capacity() > 3);
            }
            _ => panic!("size rejection returned the wrong message"),
        }
    }

    #[test]
    fn permitted_push_rejects_resource_allocation_and_returns_permit() {
        let mailbox = MessageMailbox::with_limits(1, 16, 1);
        let permit = mailbox.try_reserve().unwrap();
        let mut message = DataMessage::new(None, 0);
        message.add_resource(Arc::new(1_u64));
        message.add_resource(Arc::new(2_u64));

        let error = mailbox
            .push_with_permit(Message::Data(message), permit)
            .unwrap_err();
        assert_eq!(error.kind(), MailboxPushErrorKind::TooManyMessageResources);
        assert_eq!(mailbox.available_capacity(), 0);

        let (message, permit) = error.into_parts();
        match message {
            Message::Data(message) => assert_eq!(message.resources.len(), 2),
            _ => panic!("resource rejection returned the wrong message"),
        }
        drop(permit);
        assert_eq!(mailbox.available_capacity(), 1);
    }
}
