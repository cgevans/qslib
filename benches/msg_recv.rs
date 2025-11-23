use criterion::{black_box, criterion_group, criterion_main, Criterion};
use qslib::message_receiver::MsgRecv;

fn bench_msg_recv(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_receiving");

    // Benchmark simple message
    let simple_msg = b"OK 42 success\n";
    group.bench_function("simple_message", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            receiver.push_data(black_box(simple_msg));
            receiver.try_get_msg().unwrap()
        })
    });

    // Benchmark chunked message
    let chunk1 = b"OK 42 par";
    let chunk2 = b"tial success\n";
    group.bench_function("chunked_message", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            receiver.push_data(black_box(chunk1));
            receiver.push_data(black_box(chunk2));
            receiver.try_get_msg().unwrap()
        })
    });

    // Benchmark XML message
    let xml_msg = b"<quote>This is a\nmultiline quote\nwith some content</quote>\n";
    group.bench_function("xml_message", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            receiver.push_data(black_box(xml_msg));
            receiver.try_get_msg().unwrap()
        })
    });

    // Benchmark chunked XML message
    let xml_chunks = [
        b"<quote> ",
        b"This is ",
        b"a\nmulti-",
        b"line quo",
        b"te text\n",
    ];
    group.bench_function("chunked_xml_message", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            for chunk in xml_chunks.iter() {
                receiver.push_data(black_box(*chunk));
            }
            receiver.try_get_msg().unwrap()
        })
    });

    // Benchmark multiple messages
    let multiple_msgs = b"OK 1 first\nOK 2 second\nOK 3 third\n";
    group.bench_function("multiple_messages", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            receiver.push_data(black_box(multiple_msgs));
            let msg1 = receiver.try_get_msg().unwrap();
            let msg2 = receiver.try_get_msg().unwrap();
            let msg3 = receiver.try_get_msg().unwrap();
            (msg1, msg2, msg3)
        })
    });

    // Benchmark nested XML tags
    let nested_xml = b"<outer>Some text <inner>nested content</inner> more text</outer>\n";
    group.bench_function("nested_xml", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            receiver.push_data(black_box(nested_xml));
            receiver.try_get_msg().unwrap()
        })
    });

    // Benchmark large chunked message (1MB)
    let large_content = "x".repeat(1_000_000);
    let large_msg = format!("OK 1000 {}\n", large_content);
    let large_bytes = large_msg.as_bytes();

    // Split into ~64KB chunks
    const CHUNK_SIZE: usize = 65536;
    let large_chunks: Vec<&[u8]> = large_bytes.chunks(CHUNK_SIZE).collect();

    group.bench_function("large_chunked_message", |b| {
        b.iter(|| {
            let mut receiver = MsgRecv::new();
            for chunk in large_chunks.iter() {
                receiver.push_data(black_box(chunk));
            }
            receiver.try_get_msg().unwrap()
        })
    });

    group.finish();
}

criterion_group!(benches, bench_msg_recv);
criterion_main!(benches);
