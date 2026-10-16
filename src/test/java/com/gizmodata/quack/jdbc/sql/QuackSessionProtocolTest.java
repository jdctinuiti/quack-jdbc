package com.gizmodata.quack.jdbc.sql;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.QuackServerException;
import com.gizmodata.quack.jdbc.codec.HugeIntParts;
import com.gizmodata.quack.jdbc.message.*;
import com.gizmodata.quack.jdbc.transport.QuackTransport;
import com.gizmodata.quack.jdbc.transport.QuackUri;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeId;
import com.gizmodata.quack.jdbc.type.ExtraTypeInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class QuackSessionProtocolTest {
    private static final String V3_REQUIRED = "Unsupported Quack version - server only supports version 3 of quack";
    private static final LogicalType INTEGER = LogicalType.of(LogicalTypeId.INTEGER);
    private static final DataChunk ROW = new DataChunk(1, List.of(INTEGER),
            List.of(new DecodedVector.IntVec(INTEGER, new int[]{42}, null)));

    @Test
    void negotiatesV3OnlyAfterExplicitVersionRejection() {
        Peer peer = new Peer();
        try (QuackSession session = connect(peer, "")) {
            assertEquals(3, session.protocolVersion());
            QuackMessage.ConnectionRequest first = (QuackMessage.ConnectionRequest) peer.requests.get(0);
            QuackMessage.ConnectionRequest second = (QuackMessage.ConnectionRequest) peer.requests.get(1);
            assertEquals(Optional.of(1L), first.maxSupportedQuackVersion());
            assertTrue(first.heartbeatTimeoutSeconds().isEmpty());
            assertEquals(Optional.of(3L), second.minSupportedQuackVersion());
            assertEquals(Optional.of(60L), second.heartbeatTimeoutSeconds());
        }
    }

    @Test
    void authenticationErrorsAndExplicitV1NeverRetry() {
        for (String message : List.of("Authentication failed", V3_REQUIRED)) {
            List<QuackMessage> requests = new CopyOnWriteArrayList<>();
            QuackTransport transport = request -> {
                requests.add(request);
                throw new QuackServerException(message);
            };
            assertThrows(QuackServerException.class, () -> connect(transport,
                    message.equals(V3_REQUIRED) ? "?protocolVersion=1" : ""));
            assertEquals(1, requests.size());
        }
    }

    @Test
    void fetchesNumberedBatchesAndAcknowledgesOnlyCompleteResult() {
        Peer peer = new Peer();
        try (QuackSession session = connect(peer, "?protocolVersion=3");
             QuackSession.Cursor cursor = session.cursor("SELECT 42")) {
            assertEquals(42, cursor.nextChunk().columns().get(0).getObject(0));
            assertNull(cursor.nextChunk());
            List<QuackMessage.FetchRequest> fetches = peer.requests.stream()
                    .filter(QuackMessage.FetchRequest.class::isInstance)
                    .map(QuackMessage.FetchRequest.class::cast).toList();
            assertEquals(List.of(Optional.of(1L), Optional.of(2L)),
                    fetches.stream().map(QuackMessage.FetchRequest::batchIndex).toList());
            assertEquals(List.of(Optional.of(0L), Optional.of(1L)),
                    fetches.stream().map(QuackMessage.FetchRequest::ackIndex).toList());
            assertEquals(1, peer.count(QuackMessage.Acknowledgement.class));
            assertEquals(0, peer.count(QuackMessage.CancelRequest.class));
        }
    }

    @Test
    void rejectsMissingOrOutOfOrderBatches() {
        for (boolean badTotal : List.of(false, true)) {
            Peer peer = new Peer();
            peer.badTotal = badTotal;
            peer.badIndex = !badTotal;
            try (QuackSession session = connect(peer, "?protocolVersion=3");
                 QuackSession.Cursor cursor = session.cursor("SELECT 42")) {
                assertThrows(QuackProtocolException.class, cursor::drainAll);
                assertEquals(0, peer.count(QuackMessage.Acknowledgement.class));
            }
            assertEquals(1, peer.count(QuackMessage.CancelRequest.class));
        }
    }

    @Test
    void closingSupersededCursorCannotCancelNewQuery() {
        Peer peer = new Peer();
        try (QuackSession session = connect(peer, "?protocolVersion=3")) {
            QuackSession.Cursor old = session.cursor("SELECT 1");
            QuackSession.Cursor current = session.cursor("SELECT 2");
            old.close();
            assertEquals(0, peer.count(QuackMessage.CancelRequest.class));
            current.close();
            current.close();
            assertEquals(1, peer.count(QuackMessage.CancelRequest.class));
        }
    }

    @Test
    void appendCancelsActiveFetchBeforeSendingData() {
        Peer peer = new Peer();
        try (QuackSession session = connect(peer, "?protocolVersion=3");
             QuackSession.Cursor cursor = session.cursor("SELECT 42")) {
            session.appendChunk("main", "target", ROW);
            assertInstanceOf(QuackMessage.CancelRequest.class, peer.requests.get(2));
            assertInstanceOf(QuackMessage.PrepareRequest.class, peer.requests.get(3));
            assertEquals(1, peer.count(QuackMessage.SendDataRequest.class));
            assertEquals(1, peer.count(QuackMessage.FinalizeMessage.class));
        }
        assertEquals(1, peer.count(QuackMessage.CancelRequest.class));
    }

    @Test
    void appendDoesNotStartWhenActiveFetchCannotBeCancelled() {
        Peer peer = new Peer();
        peer.failCancel = true;
        try (QuackSession session = connect(peer, "?protocolVersion=3");
             QuackSession.Cursor cursor = session.cursor("SELECT 42")) {
            assertThrows(QuackServerException.class, () -> session.appendChunk("main", "target", ROW));
            assertEquals(0, peer.count(QuackMessage.SendDataRequest.class));
            assertTrue(session.isClosed());
        }
    }

    @Test
    void sendsHeartbeatsAndStopsOnClose() throws Exception {
        Peer peer = new Peer();
        QuackSession session = connect(peer, "?protocolVersion=3&heartbeatTimeout=1");
        try {
            assertTrue(peer.heartbeat.await(3, TimeUnit.SECONDS));
        } finally {
            session.close();
        }
        assertTrue(session.isClosed());
        assertEquals(1, peer.count(QuackMessage.DisconnectMessage.class));
        session.close();
        assertEquals(1, peer.count(QuackMessage.DisconnectMessage.class));
        assertFalse(peer.heartbeatAfterDisconnect.await(600, TimeUnit.MILLISECONDS));
    }

    @Test
    void failedHeartbeatInvalidatesSessionWithoutReplayingQueries() throws Exception {
        Peer peer = new Peer();
        peer.failHeartbeat = true;
        try (QuackSession session = connect(peer, "?protocolVersion=3&heartbeatTimeout=1")) {
            assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
                while (!session.isClosed()) Thread.sleep(10);
            });
            assertThrows(QuackProtocolException.class, () -> session.cursor("SELECT 42"));
            assertEquals(0, peer.count(QuackMessage.PrepareRequest.class));
        }
    }

    @Test
    void rejectsUnexpectedNegotiationAndCleansUpConnection() {
        Peer peer = new Peer();
        peer.selectedVersion = 2;
        assertThrows(QuackProtocolException.class, () -> connect(peer, "?protocolVersion=3"));
        assertEquals(1, peer.count(QuackMessage.DisconnectMessage.class));
    }

    private static QuackSession connect(QuackTransport transport, String params) {
        return QuackSession.connect(QuackUri.parse("jdbc:quack://example.test" + params), transport);
    }

    private static final class Peer implements QuackTransport {
        final List<QuackMessage> requests = new CopyOnWriteArrayList<>();
        final CountDownLatch heartbeat = new CountDownLatch(1);
        final CountDownLatch heartbeatAfterDisconnect = new CountDownLatch(1);
        boolean badIndex;
        boolean badTotal;
        boolean failHeartbeat;
        boolean failCancel;
        long selectedVersion = 3;

        long count(Class<?> type) {
            return requests.stream().filter(type::isInstance).count();
        }

        @Override
        public QuackMessage send(QuackMessage request) {
            requests.add(request);
            if (request instanceof QuackMessage.ConnectionRequest connect) {
                if (connect.maxSupportedQuackVersion().orElse(0L) == 1) throw new QuackServerException(V3_REQUIRED);
                return new QuackMessage.ConnectionResponse(MessageHeader.of(MessageType.CONNECTION_RESPONSE)
                        .withConnectionId("session"), Optional.empty(), Optional.empty(),
                        Optional.of(selectedVersion), connect.heartbeatTimeoutSeconds());
            }
            if (request instanceof QuackMessage.PrepareRequest prepare) {
                HugeIntParts uuid = prepare.queryUuid().orElseThrow();
                assertNotEquals(new HugeIntParts(0, 0), uuid);
                if (prepare.sql().startsWith("SELECT parameter_types FROM duckdb_functions()")) {
                    LogicalType signatureType = LogicalType.of(LogicalTypeId.LIST,
                            new ExtraTypeInfo.ListInfo(LogicalType.of(LogicalTypeId.VARCHAR), Optional.empty()));
                    DataChunk signature = new DataChunk(1, List.of(signatureType), List.of(
                            new DecodedVector.ObjectVec(signatureType, new Object[]{List.of("VARCHAR")})));
                    return new QuackMessage.PrepareResponse(MessageHeader.of(MessageType.PREPARE_RESPONSE),
                            List.of(signatureType), List.of("parameter_types"), false, List.of(signature), uuid);
                }
                return new QuackMessage.PrepareResponse(MessageHeader.of(MessageType.PREPARE_RESPONSE),
                        List.of(INTEGER), List.of("answer"), true, List.of(), uuid);
            }
            if (request instanceof QuackMessage.FetchRequest fetch) {
                long index = fetch.batchIndex().orElseThrow();
                return index == 1
                        ? new QuackMessage.FetchResponseV3(MessageHeader.of(MessageType.FETCH_RESPONSE),
                            List.of(ROW), Optional.empty(), Optional.of(badIndex ? 2L : 1L))
                        : new QuackMessage.FetchResponseV3(MessageHeader.of(MessageType.FETCH_RESPONSE),
                            List.of(), Optional.of(badTotal ? 2L : 1L), Optional.empty());
            }
            if (request instanceof QuackMessage.CancelRequest && failCancel) {
                throw new QuackServerException("Cancellation failed");
            }
            if (request instanceof QuackMessage.SendDataRequest) {
                return new QuackMessage.SendDataResponse(MessageHeader.of(MessageType.SEND_DATA_RESPONSE), Optional.empty());
            }
            if (request instanceof QuackMessage.HeartbeatRequest) {
                if (count(QuackMessage.DisconnectMessage.class) != 0) heartbeatAfterDisconnect.countDown();
                heartbeat.countDown();
                if (failHeartbeat) throw new QuackServerException("Session expired");
            }
            return new QuackMessage.SuccessResponse(MessageHeader.of(MessageType.SUCCESS_RESPONSE));
        }
    }
}
