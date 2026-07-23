use aeron_client::command::{PublicationMessageFlyweight, SubscriptionMessageFlyweight};
use aeron_client::driver_events::PublicationBuffersReadyFlyweight;
use aeron_client::uri::ChannelUri;
use agrona::AtomicBuffer;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn bench_channel_uri_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_uri");
    for uri in [
        "aeron:ipc",
        "aeron:udp?endpoint=localhost:40124|reliable=true|alias=market-data",
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(uri), &uri, |b, uri| {
            b.iter(|| ChannelUri::parse(uri).unwrap());
        });
    }
    group.finish();
}

fn bench_command_flyweights(c: &mut Criterion) {
    let channel = "aeron:udp?endpoint=localhost:40124";
    let mut publication_bytes =
        vec![0u8; PublicationMessageFlyweight::compute_length(channel.len())];
    let mut subscription_bytes =
        vec![0u8; SubscriptionMessageFlyweight::compute_length(channel.len())];

    c.bench_function("publication_command_encode_decode", |b| {
        b.iter(|| {
            let buffer = AtomicBuffer::new(&mut publication_bytes);
            let command = PublicationMessageFlyweight::new(buffer, 0);
            command.set_client_id(7);
            command.set_correlation_id(11);
            command.set_stream_id(1001);
            command.set_channel(channel);
            (
                command.client_id(),
                command.correlation_id(),
                command.stream_id(),
                command.channel(),
            )
        });
    });

    c.bench_function("subscription_command_encode_decode", |b| {
        b.iter(|| {
            let buffer = AtomicBuffer::new(&mut subscription_bytes);
            let command = SubscriptionMessageFlyweight::new(buffer, 0);
            command.set_client_id(7);
            command.set_correlation_id(11);
            command.set_registration_correlation_id(-1);
            command.set_stream_id(1001);
            command.set_channel(channel);
            (
                command.client_id(),
                command.correlation_id(),
                command.stream_id(),
                command.channel(),
            )
        });
    });

    let mut response_bytes = vec![0u8; 128];
    c.bench_function("publication_ready_response_encode_decode", |b| {
        b.iter(|| {
            let response =
                PublicationBuffersReadyFlyweight::wrap(AtomicBuffer::new(&mut response_bytes), 0);
            response.set_correlation_id(11);
            response.set_registration_id(12);
            response.set_session_id(13);
            response.set_stream_id(1001);
            response.set_log_file_name("/tmp/aeron.logbuffer");
            (response.correlation_id(), response.log_file_name().unwrap())
        });
    });
}

criterion_group!(benches, bench_channel_uri_parse, bench_command_flyweights);
criterion_main!(benches);
