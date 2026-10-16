package com.gizmodata.quack.jdbc.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises streaming without changing the server's inline-row or batch-size defaults. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("com.gizmodata.quack.jdbc.it.QuackIntegrationTest#duckdbAvailable")
class DefaultStreamingIntegrationTest {
    private QuackServerFixture server;

    @BeforeAll
    void startServer() throws Exception {
        server = QuackServerFixture.tryStart();
        assertNotNull(server);
    }

    @AfterAll
    void stopServer() {
        if (server != null) server.close();
    }

    @Test
    void defaultBatchesPreserveEveryRowAndNullableValue() throws Exception {
        try (Connection connection = DriverManager.getConnection(server.jdbcUrl());
             Statement statement = connection.createStatement()) {
            String sql = "SELECT i, 'héllo-' || i AS label, "
                    + "CASE WHEN i % 7 = 0 THEN NULL ELSE (i + 0.5)::DOUBLE END AS value, "
                    + "'\\x00\\xFF'::BLOB AS bytes FROM range(200000) t(i)";
            try (ResultSet rows = statement.executeQuery(sql)) {
                int count = 0;
                while (rows.next()) {
                    assertEquals(count, rows.getLong(1));
                    assertEquals("héllo-" + count, rows.getString(2));
                    if (count % 7 == 0) {
                        assertNull(rows.getObject(3));
                    } else {
                        assertEquals(count + 0.5, rows.getDouble(3));
                        assertFalse(rows.wasNull());
                    }
                    assertArrayEquals(new byte[]{0, (byte) 255}, rows.getBytes(4));
                    count++;
                }
                assertEquals(200_000, count);
            }
            try (ResultSet rows = statement.executeQuery("SELECT 7")) {
                assertTrue(rows.next());
                assertEquals(7, rows.getInt(1));
                assertFalse(rows.next());
            }
        }
    }

    @Test
    void closingPartialDefaultStreamAllowsAnotherCompleteQuery() throws Exception {
        try (Connection connection = DriverManager.getConnection(server.jdbcUrl() + "&requestTimeout=5");
             Statement statement = connection.createStatement()) {
            try (ResultSet rows = statement.executeQuery(
                    "SELECT i, repeat(md5(i::VARCHAR), 4) FROM range(1000000) t(i)")) {
                assertTrue(rows.next());
                assertEquals(0, rows.getLong(1));
            }
            try (ResultSet rows = statement.executeQuery("SELECT i FROM range(50000) t(i)")) {
                int count = 0;
                while (rows.next()) {
                    assertEquals(count, rows.getInt(1));
                    count++;
                }
                assertEquals(50_000, count);
            }
        }
    }
}
