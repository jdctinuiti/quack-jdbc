package com.gizmodata.quack.jdbc.message;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.codec.BinaryWriter;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MessageCodecV3Test {
    @Test
    void decodesTerminalTotalWithoutBatchIndex() {
        QuackMessage.FetchResponseV3 response = assertInstanceOf(QuackMessage.FetchResponseV3.class,
                MessageCodec.decode(fetchResponse(5L, null), 3));
        assertEquals(Optional.of(5L), response.totalBatches());
        assertTrue(response.batchIndex().isEmpty());
    }

    @Test
    void decodesVariableLengthBatchIndex() {
        QuackMessage.FetchResponseV3 response = assertInstanceOf(QuackMessage.FetchResponseV3.class,
                MessageCodec.decode(fetchResponse(null, 0x12345678L), 3));
        assertTrue(response.totalBatches().isEmpty());
        assertEquals(Optional.of(0x12345678L), response.batchIndex());
    }

    @Test
    void rejectsZeroBatchIndexAndUnsupportedMessageId() {
        assertThrows(QuackProtocolException.class, () -> MessageCodec.decode(fetchResponse(null, 0L), 3));
        assertThrows(QuackProtocolException.class, () -> MessageType.fromWireId(13));
    }

    private static byte[] fetchResponse(Long total, Long batch) {
        BinaryWriter writer = new BinaryWriter();
        writer.writeObject(header -> {
            header.writeField(1, () -> header.writeUleb(8));
            header.writeField(3, () -> header.writeUleb(-1));
        });
        writer.writeObject(body -> {
            if (batch != null) body.writeField(1, () -> body.writeUleb(1));
            if (total != null) body.writeField(2, () -> body.writeUleb(total));
            if (batch != null) body.writeField(3, () -> body.writeUleb(batch));
        });
        if (batch != null) {
            LogicalType integer = LogicalType.of(LogicalTypeId.INTEGER);
            VectorCodec.encodeDataChunk(writer, new DataChunk(1, List.of(integer),
                    List.of(new DecodedVector.IntVec(integer, new int[]{42}, null))));
        }
        return writer.toByteArray();
    }
}
