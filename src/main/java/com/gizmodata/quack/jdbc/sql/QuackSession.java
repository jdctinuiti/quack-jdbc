package com.gizmodata.quack.jdbc.sql;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.QuackServerException;
import com.gizmodata.quack.jdbc.codec.HugeIntParts;
import com.gizmodata.quack.jdbc.message.DataChunk;
import com.gizmodata.quack.jdbc.message.MessageHeader;
import com.gizmodata.quack.jdbc.message.MessageType;
import com.gizmodata.quack.jdbc.message.QuackMessage;
import com.gizmodata.quack.jdbc.transport.QuackHttpTransport;
import com.gizmodata.quack.jdbc.transport.QuackTransport;
import com.gizmodata.quack.jdbc.transport.QuackTransportFactory;
import com.gizmodata.quack.jdbc.transport.QuackUri;
import com.gizmodata.quack.jdbc.type.LogicalType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Live Quack session: connection id, query-id sequence, and a Quack transport. */
public final class QuackSession implements AutoCloseable {

    private final QuackUri uri;
    private final QuackTransport transport;
    private final AtomicLong queryIdSeq = new AtomicLong(1);
    private volatile String connectionId;
    private volatile boolean closed;
    private int protocolVersion = 1;
    private ScheduledExecutorService heartbeat;
    private volatile RuntimeException heartbeatFailure;
    private HugeIntParts activeQuery;

    public QuackSession(QuackUri uri, QuackTransport transport) {
        this.uri = Objects.requireNonNull(uri, "uri");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    public QuackSession(QuackUri uri, QuackHttpTransport transport) {
        this(uri, (QuackTransport) transport);
    }

    public static QuackSession connect(QuackUri uri) {
        return connect(uri, QuackTransportFactory.http());
    }

    public static QuackSession connect(QuackUri uri, QuackTransport transport) {
        QuackSession session = new QuackSession(uri, transport);
        try {
            session.handshake();
            return session;
        } catch (RuntimeException e) {
            session.close();
            throw e;
        }
    }

    public static QuackSession connect(QuackUri uri, QuackTransportFactory transportFactory) {
        Objects.requireNonNull(transportFactory, "transportFactory");
        return connect(uri, Objects.requireNonNull(
                transportFactory.create(uri), "transportFactory returned null"));
    }

    public QuackUri uri() {
        return uri;
    }

    public String connectionId() {
        return connectionId;
    }

    public boolean isClosed() {
        return closed || heartbeatFailure != null;
    }

    public int protocolVersion() {
        return protocolVersion;
    }

    private void handshake() {
        int requested = uri.protocolVersion();
        long timeout = uri.heartbeatTimeoutSeconds();
        try {
            handshake(requested == 0 ? 1 : requested, timeout);
        } catch (QuackServerException e) {
            // A v1 server cannot skip the additional v3 fields. Start with its exact
            // dialect and retry only the explicit v3 rejection, never an auth/HTTP error.
            if (requested != 0 || !"Unsupported Quack version - server only supports version 3 of quack"
                    .equals(e.getMessage())) throw e;
            handshake(3, timeout);
        }
    }

    private void handshake(int version, long timeout) {
        MessageHeader header = MessageHeader.of(MessageType.CONNECTION_REQUEST)
                .withClientQueryId(nextQueryId());
        QuackMessage.ConnectionRequest request = new QuackMessage.ConnectionRequest(
                header,
                uri.token(),
                Optional.of("quack-jdbc"),
                Optional.of(System.getProperty("os.name", "unknown")),
                Optional.of((long) version),
                Optional.of((long) version),
                Optional.empty(),
                version == 3 ? Optional.of(timeout) : Optional.empty());
        QuackMessage response = send(request);
        if (!(response instanceof QuackMessage.ConnectionResponse connResp)) {
            throw new QuackProtocolException(
                    "Expected CONNECTION_RESPONSE, got " + response.getClass().getSimpleName());
        }
        this.connectionId = connResp.header().connectionId().orElseThrow(
                () -> new QuackProtocolException("Server did not return a connection_id"));
        long selected = connResp.quackVersion().orElse(version == 1 ? 1L : 0L);
        if (selected != version) {
            throw new QuackProtocolException("Server selected unexpected Quack protocol version " + selected);
        }
        this.protocolVersion = version;
        if (version == 3) {
            long acceptedTimeout = connResp.heartbeatTimeoutSeconds().orElse(0L);
            if (acceptedTimeout <= 0 || acceptedTimeout > Long.MAX_VALUE / 1000) {
                throw new QuackProtocolException("Server returned invalid heartbeat timeout " + acceptedTimeout);
            }
            startHeartbeat(acceptedTimeout);
        }
    }

    private void startHeartbeat(long timeout) {
        heartbeat = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "quack-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMillis = timeout * 1000 / 3;
        heartbeat.scheduleWithFixedDelay(() -> {
            if (closed || heartbeatFailure != null) return;
            try {
                expectSuccess(send(new QuackMessage.HeartbeatRequest(header(MessageType.HEARTBEAT_REQUEST))));
            } catch (RuntimeException e) {
                if (!closed) heartbeatFailure = e;
                heartbeat.shutdown();
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void checkOpen() {
        if (heartbeatFailure != null) {
            throw new QuackProtocolException("Quack heartbeat failed; reconnect the session", heartbeatFailure);
        }
        if (closed) throw new QuackProtocolException("Session is closed");
    }

    private MessageHeader header(MessageType type) {
        return MessageHeader.of(type).withConnectionId(connectionId).withClientQueryId(nextQueryId());
    }

    private QuackMessage send(QuackMessage request) {
        try {
            QuackMessage response = transport.send(request);
            if (response instanceof QuackMessage.ErrorResponse error) {
                throw new QuackServerException(error.message(), error.exceptionType(), error.extraInfo(),
                        error.mustInvalidate());
            }
            return response;
        } catch (QuackServerException error) {
            if (error.mustInvalidate()) close();
            throw error;
        }
    }

    private static void expectSuccess(QuackMessage response) {
        if (!(response instanceof QuackMessage.SuccessResponse)) {
            throw new QuackProtocolException("Expected SUCCESS_RESPONSE, got " + response.getClass().getSimpleName());
        }
    }

    private static HugeIntParts queryUuid() {
        UUID uuid = UUID.randomUUID();
        return new HugeIntParts(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Open a streaming cursor over a prepared query. Only the initial
     * batch of chunks is fetched up front (whatever the server included
     * in {@code PREPARE_RESPONSE}); subsequent chunks are fetched on
     * demand as the caller calls {@link Cursor#nextChunk()}.
     */
    public Cursor cursor(String sql) {
        checkOpen();
        HugeIntParts uuid = protocolVersion == 3 ? queryUuid() : null;
        activeQuery = uuid;
        QuackMessage.PrepareRequest request = new QuackMessage.PrepareRequest(
                MessageHeader.of(MessageType.PREPARE_REQUEST)
                        .withConnectionId(connectionId)
                        .withClientQueryId(nextQueryId()),
                sql, Optional.ofNullable(uuid));
        QuackMessage response = send(request);
        if (!(response instanceof QuackMessage.PrepareResponse prep)) {
            throw new QuackProtocolException(
                    "Expected PREPARE_RESPONSE, got " + response.getClass().getSimpleName());
        }
        if (uuid != null && !uuid.equals(prep.resultUuid())) {
            throw new QuackProtocolException("PREPARE_RESPONSE query UUID does not match the request");
        }
        return new Cursor(this, prep);
    }

    /**
     * Append a {@link DataChunk} into {@code schema.tableName} via a Quack
     * {@code APPEND_REQUEST} (v1) or a v3 prepared insert stream.
     * The chunk's column count and types must
     * match the destination table; rows are appended atomically as a
     * single server-side batch.
     *
     * <p>This is the bulk-load fast-path — it sends column-oriented
     * binary data directly, bypassing per-row INSERT parsing. For typical
     * workloads it's an order of magnitude faster than
     * {@code PreparedStatement.executeBatch()}.
     */
    public void appendChunk(String schema, String tableName, DataChunk chunk) {
        checkOpen();
        if (tableName == null || tableName.isEmpty()) {
            throw new QuackProtocolException("appendChunk: tableName is required");
        }
        if (chunk == null) {
            throw new QuackProtocolException("appendChunk: chunk is required");
        }
        if (protocolVersion == 3) {
            try {
                // Release an unfinished result before registering the insert stream.
                if (activeQuery != null) {
                    expectSuccess(send(new QuackMessage.CancelRequest(header(MessageType.CANCEL_REQUEST), activeQuery)));
                    activeQuery = null;
                }
                appendToStream(schema, tableName, chunk);
            } catch (RuntimeException e) {
                // Do not leave a partially received insert stream alive after an uncertain failure.
                close();
                throw e;
            }
            return;
        }
        QuackMessage.AppendRequest request = new QuackMessage.AppendRequest(
                MessageHeader.of(MessageType.APPEND_REQUEST)
                        .withConnectionId(connectionId)
                        .withClientQueryId(nextQueryId()),
                Optional.ofNullable(schema),
                tableName,
                chunk);
        QuackMessage response = transport.send(request);
        if (!(response instanceof QuackMessage.SuccessResponse)) {
            throw new QuackProtocolException(
                    "Expected SUCCESS_RESPONSE for APPEND, got "
                            + response.getClass().getSimpleName());
        }
    }

    private void appendToStream(String schema, String tableName, DataChunk chunk) {
        String streamId = UUID.randomUUID().toString();
        HugeIntParts uuid = queryUuid();
        String target = schema == null || schema.isEmpty() ? quoteIdentifier(tableName)
                : quoteIdentifier(schema) + "." + quoteIdentifier(tableName);
        StringBuilder prototype = new StringBuilder("STRUCT(");
        for (int i = 0; i < chunk.types().size(); i++) {
            if (i != 0) prototype.append(", ");
            prototype.append("col").append(i).append(' ').append(SqlTypeName.render(chunk.types().get(i)));
        }
        prototype.append(')');
        String sql = "INSERT INTO " + target + " SELECT * FROM scan_data_from_quack_client("
                + SqlLiteral.render(streamId) + ", NULL::" + prototype + ", ordered := true)";
        activeQuery = uuid;
        QuackMessage response = send(new QuackMessage.PrepareRequest(header(MessageType.PREPARE_REQUEST),
                sql, Optional.of(uuid), Optional.of(0L)));
        if (!(response instanceof QuackMessage.PrepareResponse prep) || !uuid.equals(prep.resultUuid())) {
            throw new QuackProtocolException("Invalid PREPARE_RESPONSE for append stream");
        }
        try (Cursor result = new Cursor(this, prep)) {
            long batches = chunk.rowCount() == 0 ? 0 : 1;
            if (batches != 0) {
                expectSendData(send(new QuackMessage.SendDataRequest(header(MessageType.APPEND_REQUEST),
                        streamId, List.of(chunk), Optional.empty(), Optional.of(1L))));
            }
            expectSendData(send(new QuackMessage.SendDataRequest(header(MessageType.APPEND_REQUEST),
                    streamId, List.of(), Optional.of(batches), Optional.empty())));
            // SEND_DATA only accepts the bytes. Draining the statement surfaces late
            // constraint errors and waits for commit before reporting append success.
            List<DataChunk> rows = result.drainAll();
            if (rows.size() != 1 || rows.get(0).rowCount() != 1 || rows.get(0).columns().size() != 1
                    || !(rows.get(0).columns().get(0).getObject(0) instanceof Number changed)
                    || changed.longValue() != chunk.rowCount()) {
                throw new QuackProtocolException("Unexpected affected-row count for append stream");
            }
        }
    }

    private static void expectSendData(QuackMessage response) {
        if (!(response instanceof QuackMessage.SendDataResponse)) {
            throw new QuackProtocolException("Expected SEND_DATA_RESPONSE, got "
                    + response.getClass().getSimpleName());
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (heartbeat != null) heartbeat.shutdownNow();
        if (connectionId == null) {
            return;
        }
        try {
            QuackMessage.DisconnectMessage disconnect = new QuackMessage.DisconnectMessage(
                    MessageHeader.of(MessageType.DISCONNECT_MESSAGE)
                            .withConnectionId(connectionId)
                            .withClientQueryId(nextQueryId()));
            transport.send(disconnect);
        } catch (RuntimeException ignored) {
            // Best-effort disconnect.
        } finally {
            connectionId = null;
        }
    }

    private long nextQueryId() {
        return queryIdSeq.getAndIncrement();
    }

    private List<DataChunk> fetchMoreChunks(HugeIntParts resultUuid, FetchState state) {
        checkOpen();
        QuackMessage.FetchRequest fetch = new QuackMessage.FetchRequest(
                MessageHeader.of(MessageType.FETCH_REQUEST)
                        .withConnectionId(connectionId)
                        .withClientQueryId(nextQueryId()),
                resultUuid,
                protocolVersion == 3 ? Optional.of(state.nextBatch) : Optional.empty(),
                protocolVersion == 3 ? Optional.of(state.nextBatch - 1) : Optional.empty());
        QuackMessage fr = send(fetch);
        if (protocolVersion == 3) {
            if (!(fr instanceof QuackMessage.FetchResponseV3 response)) {
                throw new QuackProtocolException("Expected v3 FETCH_RESPONSE");
            }
            if (response.batchIndex().isPresent()) {
                if (response.batchIndex().get() != state.nextBatch || response.totalBatches().isPresent()) {
                    throw new QuackProtocolException("Unexpected Quack fetch batch index or premature total");
                }
                state.nextBatch++;
            } else {
                if (!response.results().isEmpty()
                        || response.totalBatches().orElse(-1L) != state.nextBatch - 1) {
                    throw new QuackProtocolException("Quack fetch ended with missing or inconsistent batches");
                }
                state.hasMore = false;
            }
            return response.results();
        }
        if (!(fr instanceof QuackMessage.FetchResponse fetchResp)) {
            throw new QuackProtocolException(
                    "Expected FETCH_RESPONSE, got " + fr.getClass().getSimpleName());
        }
        state.hasMore = fetchResp.batchIndex().isPresent();
        return fetchResp.results();
    }

    private void releaseResult(HugeIntParts uuid, boolean complete) {
        if (protocolVersion != 3 || closed || !uuid.equals(activeQuery)) return;
        activeQuery = null;
        try {
            expectSuccess(send(complete
                    ? new QuackMessage.Acknowledgement(header(MessageType.ACKNOWLEDGEMENT), uuid)
                    : new QuackMessage.CancelRequest(header(MessageType.CANCEL_REQUEST), uuid)));
        } catch (RuntimeException ignored) {
            // Result cleanup is best effort; a finished query must not fail because its ACK was lost.
        }
    }

    /**
     * Streaming cursor over the chunks of a Quack-prepared query.
     *
     * <p>Holds at most one server batch in memory at a time
     * (the batch size is controlled by the server). {@link #nextChunk()} pulls from the
     * buffered batch and issues a fresh {@code FETCH_REQUEST} when
     * the buffer is empty and the server signalled more chunks were
     * available.
     */
    public static final class Cursor implements AutoCloseable {

        private final QuackSession session;
        private final List<String> columnNames;
        private final List<LogicalType> columnTypes;
        private final HugeIntParts resultUuid;
        private final Deque<DataChunk> buffered;
        private final FetchState fetchState;
        private boolean closed;
        private int materializedRowCount;

        Cursor(QuackSession session, QuackMessage.PrepareResponse prep) {
            this.session = session;
            this.columnNames = prep.resultNames();
            this.columnTypes = prep.resultTypes();
            this.resultUuid = prep.resultUuid();
            this.buffered = new ArrayDeque<>(prep.results());
            this.fetchState = new FetchState(prep.needsMoreFetch());
            for (DataChunk c : prep.results()) materializedRowCount += c.rowCount();
        }

        public List<String> columnNames() {
            return columnNames;
        }

        public List<LogicalType> columnTypes() {
            return columnTypes;
        }

        /** Peek without advancing, fetching a batch if the server returned no inline chunks. */
        public DataChunk peekFirstChunk() {
            ensureBuffered();
            return buffered.peek();
        }

        private void ensureBuffered() {
            if (closed) return;
            while (buffered.isEmpty() && fetchState.hasMore) {
                List<DataChunk> next = session.fetchMoreChunks(resultUuid, fetchState);
                buffered.addAll(next);
                for (DataChunk c : next) materializedRowCount += c.rowCount();
            }
            if (!fetchState.hasMore) session.releaseResult(resultUuid, true);
        }

        /**
         * Return the next chunk, fetching from the server if the local
         * buffer is empty and more chunks are available. Returns
         * {@code null} when the result set is exhausted.
         */
        public DataChunk nextChunk() {
            if (closed) return null;
            ensureBuffered();
            return buffered.poll();
        }

        /** Drain every remaining chunk and return them in order. */
        public List<DataChunk> drainAll() {
            if (closed) return List.of();
            List<DataChunk> all = new ArrayList<>(buffered);
            buffered.clear();
            while (fetchState.hasMore) {
                List<DataChunk> next = session.fetchMoreChunks(resultUuid, fetchState);
                all.addAll(next);
                for (DataChunk c : next) materializedRowCount += c.rowCount();
            }
            session.releaseResult(resultUuid, true);
            return all;
        }

        /**
         * Approximate count of rows already pulled across the wire (not the
         * total result-set size — only what the cursor has seen). Useful for
         * diagnostics and tests.
         */
        public int materializedRowCount() {
            return materializedRowCount;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            buffered.clear();
            session.releaseResult(resultUuid, !fetchState.hasMore);
        }
    }

    private static final class FetchState {
        boolean hasMore;
        long nextBatch = 1;

        FetchState(boolean hasMore) {
            this.hasMore = hasMore;
        }
    }
}
