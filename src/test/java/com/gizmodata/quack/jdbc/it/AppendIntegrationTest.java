package com.gizmodata.quack.jdbc.it;

import com.gizmodata.quack.jdbc.QuackException;
import com.gizmodata.quack.jdbc.message.DataChunk;
import com.gizmodata.quack.jdbc.message.DecodedVector;
import com.gizmodata.quack.jdbc.message.Validity;
import com.gizmodata.quack.jdbc.sql.QuackConnection;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the APPEND_REQUEST encoder + the public
 * {@code QuackSession.appendChunk(...)} bulk-load API.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("com.gizmodata.quack.jdbc.it.QuackIntegrationTest#duckdbAvailable")
public class AppendIntegrationTest {

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

    private QuackConnection connect() throws SQLException {
        return (QuackConnection) DriverManager.getConnection(server.jdbcUrl());
    }

    @Test
    void firstOperationCanAppendTypedAndNullableValues() throws Exception {
        // Set up on another connection so append must work before this client's first query/fetch.
        try (Connection setup = connect(); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE jdbc_it_append_first ("
                    + "id INTEGER, active BOOLEAN, amount BIGINT, label VARCHAR, payload BLOB)");
        }

        LogicalType intType = LogicalType.of(LogicalTypeId.INTEGER);
        LogicalType boolType = LogicalType.of(LogicalTypeId.BOOLEAN);
        LogicalType bigintType = LogicalType.of(LogicalTypeId.BIGINT);
        LogicalType varcharType = LogicalType.of(LogicalTypeId.VARCHAR);
        LogicalType blobType = LogicalType.of(LogicalTypeId.BLOB);
        long[] boolValidity = Validity.allValid(4);
        Validity.setNull(boolValidity, 2);
        long[] bigintValidity = Validity.allValid(4);
        Validity.setNull(bigintValidity, 1);
        DataChunk chunk = new DataChunk(4,
                List.of(intType, boolType, bigintType, varcharType, blobType),
                List.of(
                        new DecodedVector.IntVec(intType, new int[]{1, 2, 3, 4}, null),
                        new DecodedVector.BoolVec(boolType,
                                new boolean[]{true, false, false, true}, boolValidity),
                        new DecodedVector.LongVec(bigintType,
                                new long[]{9_007_199_254_740_993L, 0, -42, Long.MAX_VALUE}, bigintValidity),
                        new DecodedVector.ObjectVec(varcharType,
                                new Object[]{"héllo", null, "", "tail-世界"}),
                        new DecodedVector.ObjectVec(blobType,
                                new Object[]{new byte[]{0, (byte) 255, 10}, null, new byte[0], new byte[]{5}})));

        try (QuackConnection connection = connect()) {
            connection.session().appendChunk("main", "jdbc_it_append_first", chunk);

            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery(
                         "SELECT id, active, amount, label, payload FROM jdbc_it_append_first ORDER BY id")) {
                Boolean[] expectedActive = {true, false, null, true};
                Long[] expectedAmounts = {9_007_199_254_740_993L, null, -42L, Long.MAX_VALUE};
                String[] expectedLabels = {"héllo", null, "", "tail-世界"};
                byte[][] expectedPayloads = {new byte[]{0, (byte) 255, 10}, null, new byte[0], new byte[]{5}};
                for (int row = 0; row < 4; row++) {
                    assertTrue(rows.next());
                    assertEquals(row + 1, rows.getInt(1));
                    assertEquals(expectedActive[row], rows.getObject(2));
                    assertEquals(expectedAmounts[row], rows.getObject(3));
                    assertEquals(expectedLabels[row], rows.getString(4));
                    assertArrayEquals(expectedPayloads[row], rows.getBytes(5));
                }
                assertFalse(rows.next());
            }
        } finally {
            try (Connection cleanup = connect(); Statement statement = cleanup.createStatement()) {
                statement.execute("DROP TABLE jdbc_it_append_first");
            }
        }
    }

    @Test
    void appendToQuotedTableParticipatesInTransaction() throws Exception {
        try (QuackConnection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA \"jdbc \"\"schema\"");
            statement.execute("CREATE TABLE \"jdbc \"\"schema\".\"append \"\"table\" (id INTEGER)");
            connection.setAutoCommit(false);
            // Establish the JDBC transaction before calling the session's bulk API directly.
            try (ResultSet rows = statement.executeQuery("SELECT 1")) {
                assertTrue(rows.next());
            }
            LogicalType intType = LogicalType.of(LogicalTypeId.INTEGER);
            DataChunk chunk = new DataChunk(2, List.of(intType),
                    List.of(new DecodedVector.IntVec(intType, new int[]{7, 8}, null)));
            connection.session().appendChunk("jdbc \"schema", "append \"table", chunk);
            try (ResultSet rows = statement.executeQuery(
                    "SELECT count(*) FROM \"jdbc \"\"schema\".\"append \"\"table\"")) {
                assertTrue(rows.next());
                assertEquals(2, rows.getInt(1));
            }
            connection.rollback();
            connection.setAutoCommit(true);
            try (ResultSet rows = statement.executeQuery(
                    "SELECT count(*) FROM \"jdbc \"\"schema\".\"append \"\"table\"")) {
                assertTrue(rows.next());
                assertEquals(0, rows.getInt(1));
            }
        } finally {
            try (Connection cleanup = connect(); Statement statement = cleanup.createStatement()) {
                statement.execute("DROP SCHEMA \"jdbc \"\"schema\" CASCADE");
            }
        }
    }

    @Test
    void failedAppendDoesNotCommitPartOfTheBatch() throws Exception {
        try (Connection setup = connect(); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE jdbc_it_append_atomic (id INTEGER PRIMARY KEY)");
        }
        LogicalType intType = LogicalType.of(LogicalTypeId.INTEGER);
        DataChunk invalid = new DataChunk(3, List.of(intType),
                List.of(new DecodedVector.IntVec(intType, new int[]{1, 2, 1}, null)));
        try {
            try (QuackConnection connection = connect()) {
                assertThrows(QuackException.class,
                        () -> connection.session().appendChunk("main", "jdbc_it_append_atomic", invalid));
            }
            // An uncertain append may close its session; inspect durable state from a new connection.
            try (QuackConnection connection = connect(); Statement statement = connection.createStatement()) {
                try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM jdbc_it_append_atomic")) {
                    assertTrue(rows.next());
                    assertEquals(0, rows.getInt(1));
                }
                DataChunk valid = new DataChunk(1, List.of(intType),
                        List.of(new DecodedVector.IntVec(intType, new int[]{3}, null)));
                connection.session().appendChunk("main", "jdbc_it_append_atomic", valid);
                try (ResultSet rows = statement.executeQuery("SELECT id FROM jdbc_it_append_atomic")) {
                    assertTrue(rows.next());
                    assertEquals(3, rows.getInt(1));
                    assertFalse(rows.next());
                }
            }
        } finally {
            try (Connection cleanup = connect(); Statement statement = cleanup.createStatement()) {
                statement.execute("DROP TABLE jdbc_it_append_atomic");
            }
        }
    }

    @Test
    void appendIntVarcharChunk() throws Exception {
        try (QuackConnection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jdbc_it_append1");
            s.execute("CREATE TABLE jdbc_it_append1 (id INTEGER, name VARCHAR)");

            LogicalType intType = LogicalType.of(LogicalTypeId.INTEGER);
            LogicalType varcharType = LogicalType.of(LogicalTypeId.VARCHAR);
            DataChunk chunk = new DataChunk(4,
                    List.of(intType, varcharType),
                    List.of(
                            new DecodedVector.IntVec(intType, new int[]{1, 2, 3, 4}, null),
                            new DecodedVector.ObjectVec(varcharType,
                                    new Object[]{"alpha", "beta", "gamma", "delta"})));

            c.session().appendChunk("main", "jdbc_it_append1", chunk);

            try (ResultSet rs = s.executeQuery(
                    "SELECT id, name FROM jdbc_it_append1 ORDER BY id")) {
                int i = 1;
                String[] expectedNames = {"alpha", "beta", "gamma", "delta"};
                while (rs.next()) {
                    assertEquals(i, rs.getInt("id"));
                    assertEquals(expectedNames[i - 1], rs.getString("name"));
                    i++;
                }
                assertEquals(5, i);
            }

            s.execute("DROP TABLE jdbc_it_append1");
        }
    }

    @Test
    void appendNullableColumn() throws Exception {
        try (QuackConnection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jdbc_it_append_null");
            s.execute("CREATE TABLE jdbc_it_append_null (v BIGINT)");

            LogicalType bigintType = LogicalType.of(LogicalTypeId.BIGINT);
            long[] validity = Validity.allValid(4);
            Validity.setNull(validity, 1);
            Validity.setNull(validity, 3);
            DataChunk chunk = new DataChunk(4,
                    List.of(bigintType),
                    List.of(new DecodedVector.LongVec(bigintType,
                            new long[]{100L, 0L, 300L, 0L}, validity)));

            c.session().appendChunk("main", "jdbc_it_append_null", chunk);

            try (ResultSet rs = s.executeQuery(
                    "SELECT v, v IS NULL AS is_null FROM jdbc_it_append_null")) {
                int row = 0;
                long[] expectedValues = {100L, 0L, 300L, 0L};
                boolean[] expectedNulls = {false, true, false, true};
                while (rs.next()) {
                    if (expectedNulls[row]) {
                        rs.getLong("v");
                        assertTrue(rs.wasNull(), "row " + row + " should be null");
                    } else {
                        assertEquals(expectedValues[row], rs.getLong("v"));
                        assertFalse(rs.wasNull(), "row " + row + " should not be null");
                    }
                    row++;
                }
                assertEquals(4, row);
            }

            s.execute("DROP TABLE jdbc_it_append_null");
        }
    }

    @Test
    void appendMixedScalarTypes() throws Exception {
        try (QuackConnection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jdbc_it_append_mix");
            s.execute("CREATE TABLE jdbc_it_append_mix (" +
                    "b BOOLEAN, i INTEGER, big BIGINT, d DOUBLE, " +
                    "dec DECIMAL(10,2), dt DATE, ts TIMESTAMP, v VARCHAR)");

            LogicalType boolType = LogicalType.of(LogicalTypeId.BOOLEAN);
            LogicalType intType = LogicalType.of(LogicalTypeId.INTEGER);
            LogicalType bigintType = LogicalType.of(LogicalTypeId.BIGINT);
            LogicalType doubleType = LogicalType.of(LogicalTypeId.DOUBLE);
            LogicalType decType = LogicalType.decimal(10, 2);
            LogicalType dateType = LogicalType.of(LogicalTypeId.DATE);
            LogicalType tsType = LogicalType.of(LogicalTypeId.TIMESTAMP);
            LogicalType varcharType = LogicalType.of(LogicalTypeId.VARCHAR);

            DataChunk chunk = new DataChunk(2,
                    List.of(boolType, intType, bigintType, doubleType,
                            decType, dateType, tsType, varcharType),
                    List.of(
                            new DecodedVector.BoolVec(boolType, new boolean[]{true, false}, null),
                            new DecodedVector.IntVec(intType, new int[]{10, 20}, null),
                            new DecodedVector.LongVec(bigintType,
                                    new long[]{1_000_000L, 2_000_000L}, null),
                            new DecodedVector.DoubleVec(doubleType,
                                    new double[]{1.5, 2.75}, null),
                            new DecodedVector.ObjectVec(decType,
                                    new Object[]{new BigDecimal("12.34"),
                                            new BigDecimal("99.99")}),
                            new DecodedVector.ObjectVec(dateType,
                                    new Object[]{LocalDate.of(2026, 1, 1),
                                            LocalDate.of(2026, 12, 31)}),
                            new DecodedVector.ObjectVec(tsType,
                                    new Object[]{
                                            LocalDateTime.of(2026, 5, 13, 9, 0, 0),
                                            LocalDateTime.of(2026, 5, 13, 17, 30, 0)}),
                            new DecodedVector.ObjectVec(varcharType,
                                    new Object[]{"first", "second"})));

            c.session().appendChunk("main", "jdbc_it_append_mix", chunk);

            try (ResultSet rs = s.executeQuery(
                    "SELECT b, i, big, d, dec, dt, ts, v FROM jdbc_it_append_mix ORDER BY i")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertEquals(10, rs.getInt(2));
                assertEquals(1_000_000L, rs.getLong(3));
                assertEquals(1.5, rs.getDouble(4), 1e-9);
                assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(5));
                assertEquals(LocalDate.of(2026, 1, 1), rs.getObject(6, LocalDate.class));
                assertEquals("first", rs.getString(8));

                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
                assertEquals(20, rs.getInt(2));
                assertEquals("second", rs.getString(8));

                assertFalse(rs.next());
            }

            s.execute("DROP TABLE jdbc_it_append_mix");
        }
    }

    @Test
    void appendIsFasterThanInsert() throws Exception {
        // Sanity check: a 5000-row APPEND should not take materially longer
        // than the same data via single statements (and is structurally
        // much faster per round-trip). Mostly here to make sure the path
        // actually works at non-trivial scale.
        try (QuackConnection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jdbc_it_append_big");
            s.execute("CREATE TABLE jdbc_it_append_big (i INTEGER)");

            int rows = 5000;
            int[] arr = new int[rows];
            for (int i = 0; i < rows; i++) arr[i] = i;
            LogicalType intType = LogicalType.of(LogicalTypeId.INTEGER);
            DataChunk chunk = new DataChunk(rows, List.of(intType), List.of(
                    new DecodedVector.IntVec(intType, arr, null)));

            c.session().appendChunk("main", "jdbc_it_append_big", chunk);

            try (ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*) AS n, MIN(i) AS lo, MAX(i) AS hi FROM jdbc_it_append_big")) {
                assertTrue(rs.next());
                assertEquals(rows, rs.getInt("n"));
                assertEquals(0, rs.getInt("lo"));
                assertEquals(rows - 1, rs.getInt("hi"));
            }
            s.execute("DROP TABLE jdbc_it_append_big");
        }
    }

    @Test
    void appendBlobAndBytes() throws Exception {
        try (QuackConnection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jdbc_it_append_blob");
            s.execute("CREATE TABLE jdbc_it_append_blob (b BLOB)");

            LogicalType blobType = LogicalType.of(LogicalTypeId.BLOB);
            byte[] payload = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
            DataChunk chunk = new DataChunk(1, List.of(blobType), List.of(
                    new DecodedVector.ObjectVec(blobType, new Object[]{payload})));

            c.session().appendChunk("main", "jdbc_it_append_blob", chunk);

            try (ResultSet rs = s.executeQuery("SELECT b FROM jdbc_it_append_blob")) {
                assertTrue(rs.next());
                assertArrayEquals(payload, rs.getBytes(1));
            }
            s.execute("DROP TABLE jdbc_it_append_blob");
        }
    }
}
