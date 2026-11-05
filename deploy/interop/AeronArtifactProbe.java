import io.aeron.AeronVersion;

/** Reject a stale or mislabelled runtime JAR before using it as interop evidence. */
public final class AeronArtifactProbe
{
    public static void main(final String[] args)
    {
        if (args.length != 2)
        {
            throw new IllegalArgumentException("expected source version and git revision");
        }

        final String expectedVersion = args[0];
        final String expectedSha = args[1];
        final String artifactSha = AeronVersion.GIT_SHA;
        if (!expectedVersion.equals(AeronVersion.VERSION) ||
            artifactSha.length() < 7 || !expectedSha.startsWith(artifactSha))
        {
            throw new IllegalStateException(
                "Aeron artifact identity mismatch: expected " + expectedVersion + "@" + expectedSha +
                ", found " + AeronVersion.VERSION + "@" + artifactSha);
        }

        System.out.printf(
            "Aeron artifact %s@%s matches source %s; JDK %s (%s)%n",
            AeronVersion.VERSION, artifactSha, expectedSha,
            System.getProperty("java.runtime.version"), System.getProperty("java.vendor"));
    }
}
