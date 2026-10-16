package com.gizmodata.quack.jdbc.it;

import com.gizmodata.quack.jdbc.sql.QuackConnection;
import com.gizmodata.quack.jdbc.message.DataChunk;
import com.gizmodata.quack.jdbc.message.DecodedVector;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeId;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("com.gizmodata.quack.jdbc.it.QuackIntegrationTest#duckdbAvailable")
class ProtocolV3IntegrationTest {
    private QuackServerFixture server;

    @BeforeAll
    void startServer() throws Exception {
        server = QuackServerFixture.tryStart();
        assertNotNull(server);
        try (QuackConnection connection = connect(); Statement statement = connection.createStatement()) {
            assumeTrue(connection.session().protocolVersion() == 3, "Requires a v3 server");
            statement.execute("SET GLOBAL quack_target_batch_bytes = 16384");
            statement.execute("SET GLOBAL quack_prepare_inline_rows = 0");
        }
    }

    @AfterAll
    void stopServer() {
        if (server != null) server.close();
    }

    private QuackConnection connect() throws Exception {
        return (QuackConnection) DriverManager.getConnection(server.jdbcUrl() + "&heartbeatTimeout=2");
    }

    @Test
    void idleConnectionOutlivesHeartbeatLease() throws Exception {
        try (QuackConnection connection = connect()) {
            Thread.sleep(5000);
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT 42")) {
                assertTrue(result.next());
                assertEquals(42, result.getInt(1));
                assertFalse(result.next());
            }
        }
    }

    @Test
    void zeroInlineRowsPreserveUpdatesTransactionsAndMetadata() throws Exception {
        try (QuackConnection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE v3_updates (id INTEGER, label VARCHAR)");
            connection.setAutoCommit(false);
            assertEquals(2, statement.executeUpdate("INSERT INTO v3_updates VALUES (1, 'one'), (2, 'two')"));
            connection.rollback();
            try (ResultSet result = statement.executeQuery("SELECT count(*) FROM v3_updates")) {
                assertTrue(result.next());
                assertEquals(0, result.getInt(1));
            }
            assertEquals(1, statement.executeUpdate("INSERT INTO v3_updates VALUES (3, 'three')"));
            connection.commit();
            try (ResultSet columns = connection.getMetaData().getColumns(null, "main", "v3_updates", "%")) {
                int count = 0;
                while (columns.next()) count++;
                assertEquals(2, count);
            }
            connection.setAutoCommit(true);
            statement.execute("DROP TABLE v3_updates");
        }
    }

    @Test
    void streamsStringsBlobsAndNullsAcrossManyBatchesThenReusesConnection() throws Exception {
        try (QuackConnection connection = connect(); Statement statement = connection.createStatement()) {
            String sql = "SELECT i, CASE WHEN i%3=0 THEN NULL ELSE 'héllo-' || i END AS txt, "
                    + "'\\x00\\xFF'::BLOB AS bytes FROM range(100000) t(i)";
            try (ResultSet rows = statement.executeQuery(sql)) {
                int count = 0;
                while (rows.next()) {
                    assertEquals(count, rows.getLong(1));
                    assertEquals(count % 3 == 0 ? null : "héllo-" + count, rows.getString(2));
                    assertArrayEquals(new byte[]{0, (byte) 255}, rows.getBytes(3));
                    count++;
                }
                assertEquals(100000, count);
            }
            // Closing a partially consumed stream must release the producer and permit another query.
            try (ResultSet rows = statement.executeQuery("SELECT i FROM range(1000000) t(i)")) {
                assertTrue(rows.next());
            }
            try (ResultSet rows = statement.executeQuery("SELECT 7")) {
                assertTrue(rows.next());
                assertEquals(7, rows.getInt(1));
                assertFalse(rows.next());
            }
        }
    }

    @Test
    void appendReleasesBackpressuredFetchOnSameConnection() throws Exception {
        try (QuackConnection connection = (QuackConnection) DriverManager.getConnection(
                server.jdbcUrl() + "&requestTimeout=3");
             Statement statement = connection.createStatement()) {
            statement.execute("SET GLOBAL quack_server_stream_buffer_bytes = 16384");
            try {
                statement.execute("CREATE TABLE v3_append_after_fetch (id INTEGER)");
                // Varying strings prevent the range from fitting in a single compressed batch.
                try (ResultSet rows = statement.executeQuery(
                        "SELECT i, md5(i::VARCHAR) FROM range(100000) t(i)")) {
                    assertTrue(rows.next());
                    LogicalType type = LogicalType.of(LogicalTypeId.INTEGER);
                    DataChunk chunk = new DataChunk(1, List.of(type),
                            List.of(new DecodedVector.IntVec(type, new int[]{42}, null)));
                    connection.session().appendChunk("main", "v3_append_after_fetch", chunk);
                }
                try (ResultSet rows = statement.executeQuery("SELECT id FROM v3_append_after_fetch")) {
                    assertTrue(rows.next());
                    assertEquals(42, rows.getInt(1));
                    assertFalse(rows.next());
                }
                statement.execute("DROP TABLE v3_append_after_fetch");
            } finally {
                if (!connection.session().isClosed()) {
                    statement.execute("RESET GLOBAL quack_server_stream_buffer_bytes");
                }
            }
        }
    }
}
