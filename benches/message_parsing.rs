use base64::{engine::general_purpose::STANDARD as b64, Engine as _};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use qslib::parser::MessageResponse;

fn create_base64_message() -> Vec<u8> {
    // Create a 5MB random payload
    let payload = vec![42u8; 5 * 1024 * 1024];
    let b64_payload = b64.encode(&payload);

    format!(
        "MESSage DataUpload -type=image/jpeg <reply.base64>{}</reply.base64>\n",
        b64_payload
    )
    .into_bytes()
}

fn create_binary_message() -> Vec<u8> {
    // Create a 5MB random payload
    let payload = vec![42u8; 5 * 1024 * 1024];

    // Format binary message with length prefix and payload
    let mut message = "MESSage DataUpload -type=image/jpeg <reply.binary>"
        .to_string()
        .into_bytes();
    message.extend(payload);
    message.extend(b"</reply.binary>\n");
    message
}

fn bench_message_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_parsing");

    // Simple message benchmark
    let simple_msg = b"MESSage Temperature -sample=22.5,22.4,22.4,22.5,22.4,22.5 -heatsink=23.4 -cover=18.0 -block=22.5,22.4,22.4,22.5,22.4,22.5\n";
    group.bench_function("simple_message", |b| {
        b.iter(|| MessageResponse::try_from(black_box(&simple_msg[..])).unwrap());
    });

    // LED status message benchmark
    let led_msg =
        b"MESSage LEDStatus Temperature:56.1434 Current:9.19802 Voltage:3.40984 JuncTemp:72.7511\n";
    group.bench_function("led_status", |b| {
        b.iter(|| MessageResponse::try_from(black_box(&led_msg[..])).unwrap());
    });

    // // Command parsing benchmark
    // let command = b"capture -exposure=100 -gain=1.0 -led=1 image.jpg";
    // group.bench_function("command", |b| {
    //     b.iter(|| {
    //         Command::try_from(black_box(&command[..])).unwrap()
    //     });
    // });

    // Binary message benchmark
    let binary_msg = create_binary_message();
    group.bench_function("binary_message", |b| {
        b.iter(|| MessageResponse::try_from(black_box(&binary_msg[..])).unwrap());
    });

    // Base64 message benchmark
    let base64_msg = create_base64_message();
    group.bench_function("base64_message", |b| {
        b.iter(|| MessageResponse::try_from(black_box(&base64_msg[..])).unwrap());
    });

    group.finish();
}

criterion_group!(benches, bench_message_parsing);
criterion_main!(benches);
