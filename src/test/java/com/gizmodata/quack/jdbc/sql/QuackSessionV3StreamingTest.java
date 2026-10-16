package com.gizmodata.quack.jdbc.sql;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.QuackServerException;
import com.gizmodata.quack.jdbc.message.*;
import com.gizmodata.quack.jdbc.transport.QuackTransport;
import com.gizmodata.quack.jdbc.transport.QuackUri;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class QuackSessionV3StreamingTest {
    private static final LogicalType INTEGER = LogicalType.of(LogicalTypeId.INTEGER);
    private static final DataChunk ROW = new DataChunk(1, List.of(INTEGER),
            List.of(new DecodedVector.IntVec(INTEGER, new int[]{42}, null)));

    @Test
    void appendFirstRegistersStreamAndWaitsForStatementCompletion() {
        Peer peer = new Peer();
        try (QuackSession session = connect(peer)) {
            session.appendChunk("a\"b", "select", ROW);
            List<QuackMessage.PrepareRequest> prepares = peer.ofType(QuackMessage.PrepareRequest.class);
            assertEquals(1, prepares.size());
            assertEquals(Optional.of(0L), prepares.get(0).inlineRows());
            assertTrue(prepares.get(0).sql().startsWith("INSERT INTO \"a\"\"b\".\"select\" SELECT * FROM"));
            assertTrue(prepares.get(0).sql().contains("NULL::STRUCT(col0 INTEGER), ordered := true)"));
            List<QuackMessage.SendDataRequest> sends = peer.ofType(QuackMessage.SendDataRequest.class);
            assertEquals(2, sends.size());
            assertEquals(sends.get(0).streamId(), sends.get(1).streamId());
            assertTrue(prepares.get(0).sql().contains(SqlLiteral.render(sends.get(0).streamId())));
            assertEquals(Optional.of(1L), sends.get(0).batchIndex());
            assertEquals(List.of(ROW), sends.get(0).chunks());
            assertTrue(sends.get(0).totalBatches().isEmpty());
            assertEquals(Optional.of(1L), sends.get(1).totalBatches());
            assertTrue(sends.get(1).chunks().isEmpty());
            assertTrue(sends.get(1).batchIndex().isEmpty());
            assertEquals(List.of(Optional.of(1L), Optional.of(2L)),
                    peer.ofType(QuackMessage.FetchRequest.class).stream().map(QuackMessage.FetchRequest::batchIndex).toList());
            assertEquals(1, peer.ofType(QuackMessage.Acknowledgement.class).size());
            assertTrue(peer.requests.indexOf(sends.get(1)) <
                    peer.requests.indexOf(peer.ofType(QuackMessage.FetchRequest.class).get(0)));
            session.appendChunk("main", "next", ROW);
            assertEquals(2, peer.ofType(QuackMessage.PrepareRequest.class).size());
            assertTrue(peer.ofType(QuackMessage.PrepareRequest.class).stream()
                    .allMatch(p -> p.sql().startsWith("INSERT INTO")));
        }
    }

    @Test
    void emptyAppendAnnouncesZeroBatchesAndVerifiesZeroAffectedRows() {
        Peer peer = new Peer();
        DataChunk empty = new DataChunk(0, List.of(INTEGER),
                List.of(new DecodedVector.IntVec(INTEGER, new int[0], null)));
        try (QuackSession session = connect(peer)) {
            session.appendChunk(null, "t", empty);
            List<QuackMessage.SendDataRequest> sends = peer.ofType(QuackMessage.SendDataRequest.class);
            assertEquals(1, sends.size());
            assertEquals(Optional.of(0L), sends.get(0).totalBatches());
            assertEquals(2, peer.ofType(QuackMessage.FetchRequest.class).size());
        }
    }

    @Test
    void terminalOrLateStatementFailureCancelsWithoutRetryOrSuccessAcknowledgement() {
        for (boolean terminal : List.of(false, true)) {
            Peer peer = new Peer();
            peer.failTerminal = terminal;
            peer.failFetch = !terminal;
            try (QuackSession session = connect(peer)) {
                QuackServerException failure = assertThrows(QuackServerException.class,
                        () -> session.appendChunk("main", "t", ROW));
                assertEquals("constraint failed", failure.getMessage());
                assertEquals(Optional.of("Constraint"), failure.getExceptionType());
                assertEquals(Map.of("constraint_name", "pk"), failure.getExtraInfo());
                assertTrue(session.isClosed());
                assertEquals(2, peer.ofType(QuackMessage.SendDataRequest.class).size());
                assertEquals(1, peer.ofType(QuackMessage.PrepareRequest.class).size());
                assertEquals(1, peer.ofType(QuackMessage.CancelRequest.class).size());
                assertTrue(peer.ofType(QuackMessage.Acknowledgement.class).isEmpty());
                assertEquals(1, peer.ofType(QuackMessage.DisconnectMessage.class).size());
            }
        }
    }

    @Test
    void fatalTypedErrorInvalidatesSession() {
        Peer peer = new Peer();
        peer.fatalPrepare = true;
        try (QuackSession session = connect(peer)) {
            QuackServerException failure = assertThrows(QuackServerException.class, () -> session.cursor("SELECT 1"));
            assertTrue(failure.mustInvalidate());
            assertTrue(session.isClosed());
            assertThrows(QuackProtocolException.class, () -> session.cursor("SELECT 2"));
            assertEquals(1, peer.ofType(QuackMessage.PrepareRequest.class).size());
        }
    }

    private static QuackSession connect(Peer peer) {
        return QuackSession.connect(QuackUri.parse("jdbc:quack://example.test?protocolVersion=3"), peer);
    }

    private static final class Peer implements QuackTransport {
        final List<QuackMessage> requests = new CopyOnWriteArrayList<>();
        boolean failTerminal;
        boolean failFetch;
        boolean fatalPrepare;
        long inserted;

        <T> List<T> ofType(Class<T> type) {
            return requests.stream().filter(type::isInstance).map(type::cast).toList();
        }

        @Override
        public QuackMessage send(QuackMessage request) {
            requests.add(request);
            if (request instanceof QuackMessage.ConnectionRequest connect) {
                return new QuackMessage.ConnectionResponse(MessageHeader.of(MessageType.CONNECTION_RESPONSE)
                        .withConnectionId("session"), Optional.empty(), Optional.empty(), Optional.of(3L),
                        connect.heartbeatTimeoutSeconds());
            }
            if (request instanceof QuackMessage.PrepareRequest prepare) {
                if (fatalPrepare) return new QuackMessage.ErrorResponse(MessageHeader.of(MessageType.ERROR_RESPONSE),
                        "database invalidated", Optional.of("Internal"), Map.of(), true);
                inserted = 0;
                return new QuackMessage.PrepareResponse(MessageHeader.of(MessageType.PREPARE_RESPONSE),
                        List.of(INTEGER), List.of("Count"), true, List.of(), prepare.queryUuid().orElseThrow());
            }
            if (request instanceof QuackMessage.SendDataRequest send) {
                inserted += send.chunks().stream().mapToLong(DataChunk::rowCount).sum();
                if (failTerminal && send.totalBatches().isPresent()) return error();
                return new QuackMessage.SendDataResponse(MessageHeader.of(MessageType.SEND_DATA_RESPONSE), Optional.empty());
            }
            if (request instanceof QuackMessage.FetchRequest fetch) {
                if (failFetch) return error();
                DataChunk count = new DataChunk(1, List.of(INTEGER),
                        List.of(new DecodedVector.IntVec(INTEGER, new int[]{(int) inserted}, null)));
                return fetch.batchIndex().orElseThrow() == 1
                        ? new QuackMessage.FetchResponseV3(MessageHeader.of(MessageType.FETCH_RESPONSE),
                            List.of(count), Optional.empty(), Optional.of(1L))
                        : new QuackMessage.FetchResponseV3(MessageHeader.of(MessageType.FETCH_RESPONSE),
                            List.of(), Optional.of(1L), Optional.empty());
            }
            return new QuackMessage.SuccessResponse(MessageHeader.of(MessageType.SUCCESS_RESPONSE));
        }

        private QuackMessage.ErrorResponse error() {
            return new QuackMessage.ErrorResponse(MessageHeader.of(MessageType.ERROR_RESPONSE), "constraint failed",
                    Optional.of("Constraint"), Map.of("constraint_name", "pk"), false);
        }
    }
}
