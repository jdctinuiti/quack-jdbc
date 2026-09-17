package com.gizmodata.quack.jdbc.message;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.codec.BinaryWriter;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MessageCodecV3Test {
    @Test
    void decodesTerminalTotalAndZeroBatchSentinel() {
        QuackMessage.FetchResponseV3 response = assertInstanceOf(QuackMessage.FetchResponseV3.class,
                MessageCodec.decode(fetchResponse(5L, 0, 8), 3));
        assertEquals(Optional.of(5L), response.totalBatches());
        assertTrue(response.batchIndex().isEmpty());
    }

    @Test
    void decodesFixedLittleEndianBatchIndex() {
        QuackMessage.FetchResponseV3 response = assertInstanceOf(QuackMessage.FetchResponseV3.class,
                MessageCodec.decode(fetchResponse(null, 0x12345678, 8), 3));
        assertTrue(response.totalBatches().isEmpty());
        assertEquals(Optional.of(0x12345678L), response.batchIndex());
    }

    @Test
    void rejectsMalformedAndUnstampedBatchIndex() {
        assertThrows(QuackProtocolException.class, () -> MessageCodec.decode(fetchResponse(null, 1, 7), 3));
        assertThrows(QuackProtocolException.class,
                () -> MessageCodec.decode(fetchResponse(null, 0xF1DCBA99C0FFEE42L, 8), 3));
    }

    private static byte[] fetchResponse(Long total, long batch, int length) {
        BinaryWriter writer = new BinaryWriter();
        writer.writeObject(header -> {
            header.writeField(1, () -> header.writeUleb(8));
            header.writeField(3, () -> header.writeUleb(-1));
        });
        writer.writeObject(body -> {
            if (total != null) body.writeField(2, () -> body.writeUleb(total));
            body.writeField(3, () -> {
                body.writeUleb(length);
                body.writeFixedInt64(batch);
            });
        });
        return writer.toByteArray();
    }
}
