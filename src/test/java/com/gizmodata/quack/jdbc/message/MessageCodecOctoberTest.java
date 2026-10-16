package com.gizmodata.quack.jdbc.message;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.codec.BinaryReader;
import com.gizmodata.quack.jdbc.codec.BinaryWriter;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MessageCodecOctoberTest {
    private static final LogicalType INTEGER = LogicalType.of(LogicalTypeId.INTEGER);
    private static final DataChunk ROW = new DataChunk(1, List.of(INTEGER),
            List.of(new DecodedVector.IntVec(INTEGER, new int[]{42}, null)));

    @Test
    void readsMultiplePlainChunksAfterTheBody() {
        QuackMessage.FetchResponseV3 response = assertInstanceOf(QuackMessage.FetchResponseV3.class,
                MessageCodec.decode(fetch(2, List.of(ROW, ROW)), 3));
        assertTrue(response.chunkBlob());
        assertEquals(Optional.of(1L), response.batchIndex());
        assertTrue(response.totalBatches().isEmpty());
        assertEquals(2, response.results().size());
        assertEquals(42, response.results().get(1).columns().get(0).getObject(0));
    }

    @Test
    void rejectsMissingExtraAndTruncatedChunks() {
        assertThrows(QuackProtocolException.class, () -> MessageCodec.decode(fetch(2, List.of(ROW)), 3));
        assertThrows(QuackProtocolException.class, () -> MessageCodec.decode(fetch(0, List.of(ROW)), 3));
        byte[] full = fetch(1, List.of(ROW));
        assertThrows(QuackProtocolException.class,
                () -> MessageCodec.decode(Arrays.copyOf(full, full.length - 1), 3));
    }

    @Test
    void acceptsZeroBatchTerminalWithoutLegacyFixedIndex() {
        BinaryWriter writer = header(8);
        writer.writeObject(body -> body.writeField(2, () -> body.writeUleb(0)));
        QuackMessage.FetchResponseV3 terminal = assertInstanceOf(QuackMessage.FetchResponseV3.class,
                MessageCodec.decode(writer.toByteArray(), 3));
        assertTrue(terminal.chunkBlob());
        assertEquals(Optional.of(0L), terminal.totalBatches());
        assertTrue(terminal.batchIndex().isEmpty());
        assertTrue(terminal.results().isEmpty());
    }

    @Test
    void readsTypedErrorsEvenDuringTheInitialV1Handshake() {
        BinaryWriter writer = header(100);
        writer.writeObject(body -> {
            body.writeField(1, () -> body.writeString("connection invalidated"));
            body.writeField(2, () -> body.writeString("INTERNAL"));
            body.writeField(3, () -> {
                body.writeUleb(1);
                body.writeObject(entry -> {
                    entry.writeField(0, () -> entry.writeString("detail"));
                    entry.writeField(1, () -> entry.writeString("test"));
                });
            });
            body.writeField(4, () -> body.writeBool(true));
        });
        for (long version : new long[]{1, 3}) {
            QuackMessage.ErrorResponse error = assertInstanceOf(QuackMessage.ErrorResponse.class,
                    MessageCodec.decode(writer.toByteArray(), version));
            assertEquals("connection invalidated", error.message());
            assertEquals(Optional.of("INTERNAL"), error.exceptionType());
            assertEquals(Map.of("detail", "test"), error.extraInfo());
            assertTrue(error.mustInvalidate());
        }
    }

    @Test
    void writesNumberedStreamDataAndSeparateTerminal() {
        byte[] bytes = MessageCodec.encode(new QuackMessage.SendDataStreamRequest(
                MessageHeader.of(MessageType.APPEND_REQUEST), "stream", List.of(ROW),
                Optional.empty(), Optional.of(1L)));
        BinaryReader reader = new BinaryReader(bytes);
        reader.readObject(() -> {
            assertEquals(9, reader.readRequiredField(1, reader::readUlebInt));
            assertEquals(-1L, reader.readRequiredField(3, reader::readUlebLong));
            return null;
        });
        reader.readObject(() -> {
            assertEquals("stream", reader.readRequiredField(1, reader::readString));
            assertEquals(1, reader.readRequiredField(2, reader::readUlebInt));
            assertEquals(1L, reader.readRequiredField(4, reader::readUlebLong));
            return null;
        });
        assertEquals(42, VectorCodec.decodeDataChunk(reader).columns().get(0).getObject(0));
        reader.assertEof();

        BinaryReader terminal = new BinaryReader(MessageCodec.encode(new QuackMessage.SendDataStreamRequest(
                MessageHeader.of(MessageType.APPEND_REQUEST), "stream", List.of(),
                Optional.of(1L), Optional.empty())));
        terminal.readObject(() -> {
            terminal.readRequiredField(1, terminal::readUlebInt);
            terminal.readRequiredField(3, terminal::readUlebLong);
            return null;
        });
        terminal.readObject(() -> {
            assertEquals("stream", terminal.readRequiredField(1, terminal::readString));
            assertEquals(1L, terminal.readRequiredField(3, terminal::readUlebLong));
            return null;
        });
        terminal.assertEof();
    }

    private static byte[] fetch(int count, List<DataChunk> chunks) {
        BinaryWriter writer = header(8);
        writer.writeObject(body -> {
            body.writeField(1, () -> body.writeUleb(count));
            body.writeField(3, () -> body.writeUleb(1));
        });
        for (DataChunk chunk : chunks) VectorCodec.encodeDataChunk(writer, chunk);
        return writer.toByteArray();
    }

    private static BinaryWriter header(int type) {
        BinaryWriter writer = new BinaryWriter();
        writer.writeObject(header -> {
            header.writeField(1, () -> header.writeUleb(type));
            header.writeField(3, () -> header.writeUleb(-1));
        });
        return writer;
    }
}
