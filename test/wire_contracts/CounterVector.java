import io.aeron.command.CounterMessageFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import java.nio.ByteBuffer;
import java.util.HexFormat;

/** Produce the ADD_COUNTER vector used by the Rust regression test. */
public final class CounterVector
{
    public static void main(final String[] args)
    {
        final byte[] bytes = new byte[64];
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.wrap(bytes));
        final CounterMessageFlyweight counter = new CounterMessageFlyweight().wrap(buffer, 0);
        counter.clientId(0x0102030405060708L);
        counter.correlationId(0x1112131415161718L);
        counter.typeId(0x21222324);
        counter.keyBuffer(new UnsafeBuffer(new byte[]{0x31, 0x32, 0x33}), 0, 3);
        counter.label("abc");
        System.out.println(HexFormat.of().formatHex(bytes, 0, counter.length()));
    }
}
