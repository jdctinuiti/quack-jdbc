package com.gizmodata.quack.jdbc.message;

import com.gizmodata.quack.jdbc.QuackProtocolException;

public enum MessageType {
    INVALID(0),
    CONNECTION_REQUEST(1),
    CONNECTION_RESPONSE(2),
    PREPARE_REQUEST(3),
    PREPARE_RESPONSE(4),
    FETCH_REQUEST(7),
    FETCH_RESPONSE(8),
    APPEND_REQUEST(9),
    SUCCESS_RESPONSE(10),
    DISCONNECT_MESSAGE(11),
    CANCEL_REQUEST(12),
    FINALIZE(13),
    SEND_DATA_RESPONSE(14),
    ACKNOWLEDGEMENT(15),
    HEARTBEAT_REQUEST(16),
    ERROR_RESPONSE(100);

    private final int wireId;

    MessageType(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static MessageType fromWireId(int wireId) {
        for (MessageType t : values()) {
            if (t.wireId == wireId) {
                return t;
            }
        }
        throw new QuackProtocolException("Unknown message type id " + wireId);
    }
}
