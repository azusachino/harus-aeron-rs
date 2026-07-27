import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

import java.nio.file.Path;

/** Holds a vendored Java MediaDriver while the Rust client probe connects to its CnC file. */
public final class JavaDriverHost
{
    private JavaDriverHost()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length != 1)
        {
            throw new IllegalArgumentException("usage: JavaDriverHost <aeron-directory>");
        }

        final Path directory = Path.of(args[0]);
        try (MediaDriver ignored = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(directory.toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)))
        {
            System.out.println(directory);
            System.out.flush();
            System.in.read();
        }
    }
}
