import io.aeron.archive.codecs.ConnectRequestEncoder;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseEncoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.codecs.StartRecordingRequestEncoder;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Generates the Archive v13 vectors asserted by the Rust codec tests.
 *
 * Compile the pinned generated codecs first with:
 * {@code vendor/aeron/gradlew -p vendor/aeron :aeron-archive:compileGeneratedJava}
 * Run Java with {@code --add-exports java.base/jdk.internal.misc=ALL-UNNAMED}.
 */
public final class ArchiveSbeGoldenVectors {
    private static final int PROTOCOL_SEMANTIC_VERSION = (1 << 16) | (12 << 8);

    public static void main(final String[] args) {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(1024));
        final MessageHeaderEncoder header = new MessageHeaderEncoder();

        final ConnectRequestEncoder connect = new ConnectRequestEncoder();
        connect.wrapAndApplyHeader(buffer, 0, header)
            .correlationId(42)
            .responseStreamId(1001)
            .version(PROTOCOL_SEMANTIC_VERSION)
            .responseChannel("aeron:ipc");
        print("connect", buffer, MessageHeaderEncoder.ENCODED_LENGTH + connect.encodedLength());

        final ControlResponseEncoder response = new ControlResponseEncoder();
        response.wrapAndApplyHeader(buffer, 0, header)
            .controlSessionId(7)
            .correlationId(43)
            .relevantId(-1)
            .code(ControlResponseCode.ERROR)
            .version(PROTOCOL_SEMANTIC_VERSION)
            .errorMessage("recording unknown");
        print("control_response", buffer, MessageHeaderEncoder.ENCODED_LENGTH + response.encodedLength());

        final StartRecordingRequestEncoder start = new StartRecordingRequestEncoder();
        start.wrapAndApplyHeader(buffer, 0, header)
            .controlSessionId(7)
            .correlationId(43)
            .streamId(1002)
            .sourceLocation(SourceLocation.LOCAL)
            .channel("aeron:udp?endpoint=127.0.0.1:40123");
        print("start_recording", buffer, MessageHeaderEncoder.ENCODED_LENGTH + start.encodedLength());
    }

    private static void print(final String name, final UnsafeBuffer buffer, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        System.out.println(name + "=" + HexFormat.of().formatHex(bytes));
    }
}
