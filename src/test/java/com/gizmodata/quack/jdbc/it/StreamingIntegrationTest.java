package com.gizmodata.quack.jdbc.it;

import com.gizmodata.quack.jdbc.message.DataChunk;
import com.gizmodata.quack.jdbc.message.DecodedVector;
import com.gizmodata.quack.jdbc.sql.QuackConnection;
import com.gizmodata.quack.jdbc.sql.QuackSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the streaming-cursor + typed-vector rewrite.
 *
 * <p>Pins two behaviors that are easy to silently regress:
 *
 * <ol>
 *   <li>{@code QuackSession.cursor()} does not materialize the entire
 *       result up front — additional FETCH_REQUESTs are issued lazily
 *       as the consumer drains chunks.</li>
 *   <li>Fixed-width primitive logical types decode into typed
 *       {@code DecodedVector} primitive-array records (no boxing).</li>
 * </ol>
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("com.gizmodata.quack.jdbc.it.QuackIntegrationTest#duckdbAvailable")
public class StreamingIntegrationTest {

    private QuackServerFixture server;

    @BeforeAll
    void startServer() throws Exception {
        server = QuackServerFixture.tryStart();
        assertNotNull(server);
        try (QuackConnection c = connect(); Statement s = c.createStatement()) {
            if (c.session().protocolVersion() == 3) {
                s.execute("SET GLOBAL quack_prepare_inline_rows = 0");
                s.execute("SET GLOBAL quack_target_batch_bytes = 16384");
            }
        }
    }

    @AfterAll
    void stopServer() {
        if (server != null) server.close();
    }

    private QuackConnection connect() throws SQLException {
        return (QuackConnection) DriverManager.getConnection(server.jdbcUrl());
    }

    @Test
    void cursorFetchesLazilyForLargeResult() throws Exception {
        // 200k-row range query is large enough to exceed the server's
        // default batch (quack_fetch_batch_chunks=12) but small enough to
        // finish quickly. The cursor must not hold the whole thing in
        // memory after the initial PREPARE_RESPONSE.
        try (QuackConnection c = connect();
             QuackSession.Cursor cursor = c.session().cursor("SELECT i FROM range(0, 200000) t(i)")) {

            int initialRows = cursor.materializedRowCount();
            if (c.session().protocolVersion() == 1) {
                assertTrue(initialRows > 0, "V1 PREPARE_RESPONSE should include an initial chunk");
            }
            assertTrue(initialRows < 200_000,
                    "cursor should not have materialized the full result up front; saw "
                            + initialRows + " rows already");

            // Walk a handful of chunks and confirm the materialized counter
            // increases monotonically (proves we're actually pulling more
            // batches over the wire as we drain).
            int prev = initialRows;
            int rowsSeen = 0;
            for (int i = 0; i < 8; i++) {
                DataChunk chunk = cursor.nextChunk();
                if (chunk == null) break;
                rowsSeen += chunk.rowCount();
                assertTrue(cursor.materializedRowCount() >= prev,
                        "materializedRowCount should not decrease");
                prev = cursor.materializedRowCount();
            }
            assertTrue(rowsSeen > 0);
        }
    }

    @Test
    void resultSetStreamsThroughEntireRange() throws Exception {
        // End-to-end smoke: walk all 100k rows through the JDBC ResultSet
        // surface and verify exact count + monotonic increasing values
        // (proves no chunk was dropped or duplicated across the FETCH loop).
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT i FROM range(0, 100000) t(i)")) {
            long expected = 0L;
            int count = 0;
            while (rs.next()) {
                assertEquals(expected, rs.getLong(1), "row mismatch at " + count);
                expected++;
                count++;
            }
            assertEquals(100_000, count);
        }
    }

    @Test
    void primitiveLogicalTypesProduceTypedVectors() throws Exception {
        try (QuackConnection c = connect();
             QuackSession.Cursor cursor = c.session().cursor(
                     "SELECT 1::BOOLEAN AS b, "
                             + "2::TINYINT AS i8, "
                             + "3::SMALLINT AS i16, "
                             + "4::INTEGER AS i32, "
                             + "5::BIGINT AS i64, "
                             + "1.5::FLOAT AS f, "
                             + "2.5::DOUBLE AS d, "
                             + "'hello'::VARCHAR AS v")) {
            DataChunk chunk = cursor.nextChunk();
            assertNotNull(chunk, "expected at least one chunk");
            assertEquals(8, chunk.columns().size());

            assertColumnVecType(chunk, 0, DecodedVector.BoolVec.class);
            assertColumnVecType(chunk, 1, DecodedVector.ByteVec.class);
            assertColumnVecType(chunk, 2, DecodedVector.ShortVec.class);
            assertColumnVecType(chunk, 3, DecodedVector.IntVec.class);
            assertColumnVecType(chunk, 4, DecodedVector.LongVec.class);
            assertColumnVecType(chunk, 5, DecodedVector.FloatVec.class);
            assertColumnVecType(chunk, 6, DecodedVector.DoubleVec.class);
            // VARCHAR materializes to String objects; ObjectVec is the right home.
            assertColumnVecType(chunk, 7, DecodedVector.ObjectVec.class);
        }
    }

    @Test
    void datesAndDecimalsStayInObjectVec() throws Exception {
        try (QuackConnection c = connect();
             QuackSession.Cursor cursor = c.session().cursor(
                     "SELECT DATE '2026-05-13' AS d, "
                             + "TIMESTAMP '2026-05-13 14:00:00' AS ts, "
                             + "123.45::DECIMAL(10,2) AS dec, "
                             + "'00000000-0000-0000-0000-000000000001'::UUID AS u, "
                             + "INTERVAL '5 days' AS iv")) {
            DataChunk chunk = cursor.nextChunk();
            assertNotNull(chunk);
            for (int i = 0; i < chunk.columns().size(); i++) {
                assertTrue(chunk.columns().get(i) instanceof DecodedVector.ObjectVec,
                        "expected ObjectVec for column " + i
                                + ", got " + chunk.columns().get(i).getClass().getSimpleName());
            }
        }
    }

    @Test
    void sequenceEncodingProducesTypedVector() throws Exception {
        // range() returns BIGINT and DuckDB encodes it as a SEQUENCE vector.
        // We must materialize that as a LongVec (primitive long[]), not
        // ObjectVec — otherwise the memory benefit vanishes for SELECT i FROM range().
        try (QuackConnection c = connect();
             QuackSession.Cursor cursor = c.session().cursor("SELECT i FROM range(0, 1000) t(i)")) {
            DataChunk chunk = cursor.nextChunk();
            assertNotNull(chunk);
            assertTrue(chunk.columns().get(0) instanceof DecodedVector.LongVec,
                    "expected SEQUENCE → LongVec, got "
                            + chunk.columns().get(0).getClass().getSimpleName());
            // Spot-check a couple of values
            DecodedVector.LongVec vec = (DecodedVector.LongVec) chunk.columns().get(0);
            assertEquals(0L, vec.values()[0]);
            if (vec.size() > 5) assertEquals(5L, vec.values()[5]);
        }
    }

    @Test
    void constantEncodingProducesTypedVector() throws Exception {
        // SELECT <literal> FROM range(n) often emits a CONSTANT-encoded vector.
        // Whatever the server chooses, we should land on an IntVec for a
        // literal INTEGER repeated across rows.
        try (QuackConnection c = connect();
             QuackSession.Cursor cursor = c.session().cursor("SELECT 42::INTEGER AS x FROM range(0, 1000)")) {
            DataChunk chunk = cursor.nextChunk();
            assertNotNull(chunk);
            // Either FLAT or CONSTANT, both must land in IntVec.
            assertTrue(chunk.columns().get(0) instanceof DecodedVector.IntVec,
                    "expected typed IntVec, got "
                            + chunk.columns().get(0).getClass().getSimpleName());
            DecodedVector.IntVec vec = (DecodedVector.IntVec) chunk.columns().get(0);
            assertEquals(42, vec.values()[0]);
            assertEquals(42, vec.values()[vec.size() - 1]);
        }
    }

    private static void assertColumnVecType(DataChunk chunk, int column,
                                            Class<? extends DecodedVector> expected) {
        DecodedVector actual = chunk.columns().get(column);
        assertTrue(expected.isInstance(actual),
                "column " + column + " expected " + expected.getSimpleName()
                        + ", got " + actual.getClass().getSimpleName());
    }
}
