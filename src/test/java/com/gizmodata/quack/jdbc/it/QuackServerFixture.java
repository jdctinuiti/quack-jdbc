package com.gizmodata.quack.jdbc.it;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Spawns a real DuckDB CLI as a Quack server for integration tests.
 *
 * <p>Skips (returns {@code null} from {@link #tryStart}) when the
 * {@code duckdb} binary cannot be located, so unit-only test runs still work
 * on machines without DuckDB installed. Set {@code QUACK_IT_DUCKDB} to
 * override the duckdb binary path.
 */
public final class QuackServerFixture implements AutoCloseable {

    private static final String DEFAULT_TOKEN = "quack-jdbc-it-token";

    private final Process process;
    private final int port;
    private final String token;
    private final BufferedWriter stdin;
    private final Thread stdoutPump;
    private final Path logFile;

    private QuackServerFixture(Process process, int port, String token,
                               BufferedWriter stdin, Thread stdoutPump, Path logFile) {
        this.process = process;
        this.port = port;
        this.token = token;
        this.stdin = stdin;
        this.stdoutPump = stdoutPump;
        this.logFile = logFile;
    }

    public static QuackServerFixture tryStart() throws IOException, InterruptedException {
        String duckdb = System.getenv().getOrDefault("QUACK_IT_DUCKDB", "duckdb");
        if (!isOnPath(duckdb)) {
            return null;
        }

        int port = pickFreePort();
        String token = DEFAULT_TOKEN;
        Path logFile = Files.createTempFile("quack-jdbc-it-", ".log");

        String extensionDir = System.getenv("QUACK_IT_EXTENSION_DIR");
        ProcessBuilder pb = extensionDir == null ? new ProcessBuilder(duckdb)
                : new ProcessBuilder(duckdb, "-unsigned");
        pb.redirectErrorStream(true);
        Process process = pb.start();

        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));

        ExecutorService logger = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "quack-server-logger");
            t.setDaemon(true);
            return t;
        });
        BufferedReader stdout = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        BufferedWriter logWriter = Files.newBufferedWriter(logFile);
        Thread pump = new Thread(() -> {
            try {
                String line;
                while ((line = stdout.readLine()) != null) {
                    logWriter.write(line);
                    logWriter.newLine();
                    logWriter.flush();
                }
            } catch (IOException ignored) {
            } finally {
                try { logWriter.close(); } catch (IOException ignored) {}
            }
        }, "quack-server-stdout");
        pump.setDaemon(true);
        pump.start();
        logger.shutdown();

        try {
            writer.write(".mode csv\n");
            writer.write(".headers on\n");
            if (extensionDir == null) {
                writer.write("INSTALL quack;\n");
                writer.write("LOAD quack;\n");
            } else {
                Path extensions = Path.of(extensionDir).toAbsolutePath();
                if (Files.exists(extensions.resolve("httpfs.duckdb_extension"))) {
                    writer.write("LOAD '" + extensions.resolve("httpfs.duckdb_extension").toString().replace("'", "''") + "';\n");
                }
                writer.write("LOAD '" + extensions.resolve("quack.duckdb_extension").toString().replace("'", "''") + "';\n");
            }
            writer.write("CALL quack_serve('quack:127.0.0.1:" + port + "', token=>'" + token + "');\n");
            writer.flush();
        } catch (IOException e) {
            process.destroyForcibly();
            throw e;
        }

        if (!waitForReady("127.0.0.1", port, 60_000)) {
            process.destroyForcibly();
            throw new IOException("Quack server did not become ready within 60s. See log: " + logFile);
        }
        return new QuackServerFixture(process, port, token, writer, pump, logFile);
    }

    public String jdbcUrl() {
        return "jdbc:quack://127.0.0.1:" + port + "?token=" + token;
    }

    public int port() {
        return port;
    }

    public String token() {
        return token;
    }

    public Path logFile() {
        return logFile;
    }

    @Override
    public void close() {
        try {
            stdin.write("CALL quack_stop('quack:127.0.0.1:" + port + "');\n");
            stdin.write(".quit\n");
            stdin.flush();
        } catch (IOException ignored) {
        }
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
        stdoutPump.interrupt();
    }

    // ---- helpers ----

    private static int pickFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static boolean waitForReady(String host, int port, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException ignored) {
                Thread.sleep(250);
            }
        }
        return false;
    }

    private static boolean isOnPath(String binary) {
        if (Path.of(binary).isAbsolute()) return Files.isExecutable(Path.of(binary));
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return false;
        for (String entry : pathEnv.split(java.io.File.pathSeparator)) {
            Path candidate = Path.of(entry, binary);
            if (Files.isExecutable(candidate)) {
                return true;
            }
        }
        return false;
    }
}
