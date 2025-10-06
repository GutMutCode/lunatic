use criterion::{criterion_group, criterion_main, Criterion};
use lunatic_process::mailbox::MessageMailbox;
use lunatic_process::message::Message;
use tokio::runtime::Runtime;

fn bench_message_round_trip(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("message_round_trip", |b| {
        b.to_async(&rt).iter(|| async {
            let mailbox = MessageMailbox::default();
            let message = Message::LinkDied(Some(1));
            mailbox.push(message);
            let _ = mailbox.pop(None).await;
        });
    });
}

fn bench_selective_round_trip(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("message_round_trip_selective", |b| {
        b.to_async(&rt).iter(|| async {
            let mailbox = MessageMailbox::default();
            for i in 0..32 {
                mailbox.push(Message::LinkDied(Some(i)));
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
