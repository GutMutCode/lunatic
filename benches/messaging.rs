use criterion::{criterion_group, criterion_main, Criterion};
use lunatic_process::{
    message::Message,
    state::{mailboxes_with_capacity, SignalEnvelope},
    Signal,
};
use tokio::runtime::Runtime;

fn queued_message(envelope: SignalEnvelope) -> (Message, lunatic_process::mailbox::MailboxPermit) {
    let (signal, permit) = envelope.into_parts();
    let Signal::Message(message) = signal else {
        unreachable!("messaging benchmark only queues message signals")
    };
    (
        message,
        permit.expect("message signals reserve mailbox capacity"),
    )
}

fn bench_message_round_trip(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("message_round_trip", |b| {
        b.to_async(&rt).iter(|| async {
            let ((sender, receiver), mailbox) = mailboxes_with_capacity(1, 1);
            let message = Message::LinkDied(Some(1));
            sender.send(Signal::Message(message)).unwrap();
            let (message, permit) = queued_message(receiver.recv().await.unwrap());
            mailbox.push_with_permit(message, permit).unwrap();
            let _ = mailbox.pop(None).await;
        });
    });
}

fn bench_selective_round_trip(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("message_round_trip_selective", |b| {
        b.to_async(&rt).iter(|| async {
            // The signal ingress reserves one physical slot for lifecycle
            // control, so 33 slots admit this 32-message data burst.
            let ((sender, receiver), mailbox) = mailboxes_with_capacity(33, 32);
            for i in 0..32 {
                sender
                    .send(Signal::Message(Message::LinkDied(Some(i))))
                    .unwrap();
            }
            for _ in 0..32 {
                let (message, permit) = queued_message(receiver.recv().await.unwrap());
                mailbox.push_with_permit(message, permit).unwrap();
            }
            let tags = [31i64];
            let _ = mailbox.pop(Some(&tags)).await;
        });
    });
}

criterion_group!(
    benches,
    bench_message_round_trip,
    bench_selective_round_trip
);
criterion_main!(benches);
