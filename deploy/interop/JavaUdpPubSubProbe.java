import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

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
        if (args.length != 2 && args.length != 3)
        {
            throw new IllegalArgumentException(
                "usage: JavaUdpPubSubProbe <aeron-directory> <port> [message-count]");
        }

        final String channel = "aeron:udp?endpoint=127.0.0.1:" + args[1];
        final int messageCount = args.length == 3 ? parseMessageCount(args[2]) : 1;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(Path.of(args[0]).toString()));
            Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            await("UDP publication/subscription connection", TIMEOUT_NS, ()
                -> publication.isConnected() && subscription.isConnected());

            final long deadline = System.nanoTime() + TIMEOUT_NS;
            int offered = 0;
            final int[] received = {0};
            while (received[0] < messageCount && System.nanoTime() < deadline)
            {
                if (offered < messageCount)
                {
                    final byte[] payload = payloadFor(offered);
                    final long position = publication.offer(new UnsafeBuffer(payload), 0, payload.length);
                    if (position > 0)
                    {
                        offered++;
                    }
                    else if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED)
                    {
                        throw new IllegalStateException("UDP publication offer failed: " + position);
                    }
                }

                subscription.poll((buffer, offset, length, header) ->
                {
                    final byte[] value = new byte[length];
                    buffer.getBytes(offset, value);
                    final byte[] expected = payloadFor(received[0]);
                    if (!Arrays.equals(expected, value))
                    {
                        throw new IllegalStateException(
                            "unexpected UDP payload at sequence " + received[0]);
                    }
                    received[0]++;
                }, 10);
                Thread.yield();
            }

            if (received[0] != messageCount)
            {
                throw new IllegalStateException(
                    "UDP publication timed out: offered=" + offered + ", received=" + received[0] +
                    ", expected=" + messageCount);
            }
            System.out.println("Java Aeron UDP round-trip through Rust driver passed: messages=" + messageCount);
        }
    }

    private static int parseMessageCount(final String value)
    {
        final int messageCount;
        try
        {
            messageCount = Integer.parseInt(value);
        }
        catch (final NumberFormatException ex)
        {
            throw new IllegalArgumentException("message-count must be a positive integer", ex);
        }
        if (messageCount <= 0)
        {
            throw new IllegalArgumentException("message-count must be a positive integer");
        }
        return messageCount;
    }

    private static byte[] payloadFor(final int sequence)
    {
        final byte[] payload = Arrays.copyOf(PAYLOAD, PAYLOAD.length + Long.BYTES);
        long value = sequence;
        for (int index = 0; index < Long.BYTES; index++)
        {
            payload[PAYLOAD.length + index] = (byte)value;
            value >>>= Byte.SIZE;
        }
        return payload;
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
