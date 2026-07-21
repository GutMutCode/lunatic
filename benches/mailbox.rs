use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use lunatic_process::mailbox::MessageMailbox;
use lunatic_process::message::Message;

fn bench_selective_receive(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_selective_receive");

    let rt = tokio::runtime::Runtime::new().unwrap();

    for num_messages in [10, 100, 1000].iter() {
        for num_tags in [1, 5, 10].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("msg={}_tags={}", num_messages, num_tags)),
                &(num_messages, num_tags),
                |b, &(&n_msg, &n_tags)| {
                    b.to_async(&rt).iter(|| async {
                        let mailbox = MessageMailbox::default();

                        for i in 0..n_msg {
                            mailbox.push(Message::LinkDied(Some(i))).unwrap();
                        }

                        let tags: Vec<i64> = (n_msg - n_tags..n_msg).collect();

                        let _message = mailbox.pop(Some(&tags)).await;
                        black_box(_message);
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_fifo_receive(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_fifo_receive");

    let rt = tokio::runtime::Runtime::new().unwrap();

    for num_messages in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("msg={}", num_messages)),
            num_messages,
            |b, &n_msg| {
                b.to_async(&rt).iter(|| async {
                    let mailbox = MessageMailbox::default();

                    for i in 0..n_msg {
                        mailbox.push(Message::LinkDied(Some(i))).unwrap();
                    }

                    let _message = mailbox.pop(None).await;
                    black_box(_message);
                });
            },
        );
    }

    group.finish();
}

fn bench_worst_case_selective(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_worst_case");

    let rt = tokio::runtime::Runtime::new().unwrap();

    for num_messages in [100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("msg={}", num_messages)),
            num_messages,
            |b, &n_msg| {
                b.to_async(&rt).iter(|| async {
                    let mailbox = MessageMailbox::default();

                    for i in 0..n_msg {
                        mailbox.push(Message::LinkDied(Some(i))).unwrap();
                    }

                    // Match the final queued message so `pop` scans the whole
                    // mailbox without waiting forever for a missing tag.
                    let tags: Vec<i64> = vec![n_msg - 1];

                    let _message = mailbox.pop(Some(&tags)).await;
                    black_box(_message);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_selective_receive,
    bench_fifo_receive,
    bench_worst_case_selective
);
criterion_main!(benches);
