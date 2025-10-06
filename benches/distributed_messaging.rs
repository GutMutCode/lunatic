use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lunatic_distributed::distributed::message::Request;

fn bench_request_encode_decode(c: &mut Criterion) {
    // Measure serialization cost for a representative distributed message payload
    c.bench_function("distributed_request_encode_1kb", |b| {
        b.iter_batched(
            || vec![0u8; 1024],
            |payload| {
                let request = Request::Message {
                    node_id: 1,
                    environment_id: 42,
                    process_id: 99,
                    tag: Some(7),
                    data: payload,
                };
                let encoded = rmp_serde::to_vec(&request).expect("encode");
                black_box(encoded);
            },
            BatchSize::SmallInput,
        );
    });

    let template = Request::Message {
        node_id: 1,
        environment_id: 42,
        process_id: 99,
        tag: Some(7),
        data: vec![0u8; 1024],
    };
    let encoded = rmp_serde::to_vec(&template).expect("encode template");

    c.bench_function("distributed_request_decode_1kb", |b| {
        b.iter(|| {
            let decoded: Request = rmp_serde::from_slice(black_box(&encoded)).expect("decode");
            black_box(decoded);
        });
    });
}

criterion_group!(benches, bench_request_encode_decode);
criterion_main!(benches);
