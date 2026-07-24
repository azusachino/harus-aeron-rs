import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** Runs a Java Aeron UDP round-trip through the Rust MediaDriver. */
public final class JavaUdpPubSubProbe
{
    private static final int STREAM_ID = 778;
    private static final byte[] PAYLOAD = "java-udp-through-rust-driver".getBytes(StandardCharsets.UTF_8);
    private static final long TIMEOUT_NS = 15_000_000_000L;

    private JavaUdpPubSubProbe()
    {
    }

    public static void main(final String[] args)
    {
        if (args.length != 2)
        {
            throw new IllegalArgumentException("usage: JavaUdpPubSubProbe <aeron-directory> <port>");
        }

        final String channel = "aeron:udp?endpoint=127.0.0.1:" + args[1];
        final UnsafeBuffer offerBuffer = new UnsafeBuffer(PAYLOAD);
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(Path.of(args[0]).toString()));
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            await("UDP publication/subscription connection", TIMEOUT_NS, ()
                -> publication.isConnected() && subscription.isConnected());

            final long deadline = System.nanoTime() + TIMEOUT_NS;
            long position = Publication.NOT_CONNECTED;
            while (position < 0 && System.nanoTime() < deadline)
            {
                position = publication.offer(offerBuffer, 0, PAYLOAD.length);
                Thread.yield();
            }
            if (position < 0)
            {
                throw new IllegalStateException("UDP publication offer failed: " + position);
            }

            final int[] fragments = {0};
            final byte[][] received = {null};
            await("UDP publication delivery", TIMEOUT_NS, () ->
            {
                subscription.poll((buffer, offset, length, header) ->
                {
                    final byte[] value = new byte[length];
                    buffer.getBytes(offset, value);
                    received[0] = value;
                    fragments[0]++;
                }, 10);
                return fragments[0] == 1;
            });

            final String result = new String(received[0], StandardCharsets.UTF_8);
            if (!result.equals(new String(PAYLOAD, StandardCharsets.UTF_8)))
            {
                throw new IllegalStateException("unexpected UDP payload: " + result);
            }
            System.out.println("Java Aeron UDP round-trip through Rust driver passed");
        }
    }

    private static void await(final String operation, final long timeoutNs, final Check check)
    {
        final long deadline = System.nanoTime() + timeoutNs;
        while (!check.ok() && System.nanoTime() < deadline)
        {
            Thread.yield();
        }
        if (!check.ok())
        {
            throw new IllegalStateException("timed out waiting for " + operation);
        }
    }

    @FunctionalInterface
    private interface Check
    {
        boolean ok();
    }
}
