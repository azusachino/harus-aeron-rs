import io.aeron.CncFileDescriptor;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.agrona.concurrent.status.CountersReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Verifies that a Rust-generated cnc.dat can be consumed by Aeron Java's CnC readers. */
public final class CncLayoutProbe
{
    private static final int CONTROL_PROTOCOL_COUNTER_ID = 43;
    private static final int DRIVER_SYSTEM_COUNTER_TYPE_ID = 8;
    private static final long CONTROL_PROTOCOL_VERSION = 0x01_00_00_00L;

    private CncLayoutProbe()
    {
    }

    public static void main(final String[] args) throws IOException
    {
        if (args.length != 1)
        {
            throw new IllegalArgumentException("usage: CncLayoutProbe <cnc.dat>");
        }

        final Path path = Path.of(args[0]);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            final ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
            final UnsafeBuffer metadata = CncFileDescriptor.createMetaDataBuffer(mapped);
            CncFileDescriptor.checkVersion(metadata.getInt(CncFileDescriptor.CNC_VERSION_FIELD_OFFSET));

            final UnsafeBuffer countersMetadata =
                CncFileDescriptor.createCountersMetaDataBuffer(mapped, metadata);
            final UnsafeBuffer countersValues =
                CncFileDescriptor.createCountersValuesBuffer(mapped, metadata);
            final CountersReader counters = new CountersReader(countersMetadata, countersValues);
            if (RingBufferDescriptor.CONSUMER_HEARTBEAT_OFFSET != 640 ||
                RingBufferDescriptor.TRAILER_LENGTH != 768)
            {
                throw new IllegalStateException(
                    "unexpected Agrona ring layout: heartbeat=" +
                    RingBufferDescriptor.CONSUMER_HEARTBEAT_OFFSET +
                    ", trailer=" + RingBufferDescriptor.TRAILER_LENGTH);
            }
            final ManyToOneRingBuffer toDriver =
                new ManyToOneRingBuffer(CncFileDescriptor.createToDriverBuffer(mapped, metadata));

            if (toDriver.capacity() != (1 << 20))
            {
                throw new IllegalStateException("unexpected to-driver data capacity: " + toDriver.capacity());
            }

            if (toDriver.consumerHeartbeatTime() == 0)
            {
                throw new IllegalStateException("driver heartbeat is missing");
            }

            if (counters.getCounterState(CONTROL_PROTOCOL_COUNTER_ID) != CountersReader.RECORD_ALLOCATED)
            {
                throw new IllegalStateException("control protocol counter is not allocated");
            }
            if (counters.getCounterTypeId(CONTROL_PROTOCOL_COUNTER_ID) != DRIVER_SYSTEM_COUNTER_TYPE_ID)
            {
                throw new IllegalStateException("control protocol counter has the wrong type");
            }
            if (counters.getCounterRegistrationId(CONTROL_PROTOCOL_COUNTER_ID) != CONTROL_PROTOCOL_COUNTER_ID)
            {
                throw new IllegalStateException(
                    "control protocol counter has the wrong registration id: " +
                    counters.getCounterRegistrationId(CONTROL_PROTOCOL_COUNTER_ID));
            }
            if (counters.getCounterValue(CONTROL_PROTOCOL_COUNTER_ID) != CONTROL_PROTOCOL_VERSION)
            {
                throw new IllegalStateException("control protocol counter has the wrong value");
            }

            System.out.println("Java CnC layout probe passed");
        }
    }
}
