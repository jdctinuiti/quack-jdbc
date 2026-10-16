package com.gizmodata.quack.jdbc.message;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.codec.BinaryReader;
import com.gizmodata.quack.jdbc.codec.BinaryWriter;
import com.gizmodata.quack.jdbc.codec.HugeIntParts;
import com.gizmodata.quack.jdbc.codec.QuackConstants;
import com.gizmodata.quack.jdbc.type.LogicalType;
import com.gizmodata.quack.jdbc.type.LogicalTypeCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class MessageCodec {

    private MessageCodec() {
    }

    public static byte[] encode(QuackMessage message) {
        BinaryWriter writer = new BinaryWriter(256);
        encodeHeader(writer, message.header(), message);
        encodeBody(writer, message);
        return writer.toByteArray();
    }

    public static QuackMessage decode(byte[] bytes) {
        return decode(bytes, 1);
    }

    public static QuackMessage decode(byte[] bytes, long protocolVersion) {
        if (protocolVersion != 1 && protocolVersion != 3) {
            throw new QuackProtocolException("Unsupported Quack protocol version " + protocolVersion);
        }
        BinaryReader reader = new BinaryReader(bytes);
        DecodedHeader decoded = decodeHeader(reader);
        if (protocolVersion == 3 && (decoded.header().type() == MessageType.FETCH_RESPONSE
                || decoded.header().type() == MessageType.APPEND_REQUEST)) {
            // Both layouts advertise version 3. Parse complete frames, including EOF, rather
            // than guessing from a DuckDB version or replaying a request after a decode error.
            try {
                QuackMessage message = decoded.header().type() == MessageType.FETCH_RESPONSE
                        ? decodeChunkBlobFetch(reader, decoded.header()) : decodeChunkBlobSend(reader, decoded.header());
                reader.assertEof();
                return message;
            } catch (QuackProtocolException blobFailure) {
                BinaryReader legacy = new BinaryReader(bytes);
                DecodedHeader legacyHeader = decodeHeader(legacy);
                try {
                    QuackMessage message = decodeBody(legacy, legacyHeader, protocolVersion);
                    legacy.assertEof();
                    return message;
                } catch (QuackProtocolException legacyFailure) {
                    blobFailure.addSuppressed(legacyFailure);
                    throw blobFailure;
                }
            }
        }
        QuackMessage message = decodeBody(reader, decoded, protocolVersion);
        reader.assertEof();
        return message;
    }

    private static void encodeHeader(BinaryWriter writer, MessageHeader header, QuackMessage message) {
        writer.writeObject(obj -> {
            obj.writeField(1, () -> obj.writeUleb(messageType(message).wireId()));
            header.connectionId().ifPresent(id ->
                    obj.writeField(2, () -> obj.writeString(id)));
            obj.writeField(3, () -> obj.writeUleb(
                    header.clientQueryId().orElse(QuackConstants.OPTIONAL_INDEX_INVALID)));
        });
    }

    private static DecodedHeader decodeHeader(BinaryReader reader) {
        return reader.readObject(() -> {
            MessageType type = MessageType.fromWireId(
                    reader.readRequiredField(1, reader::readUlebInt));
            Optional<String> connectionId = Optional.ofNullable(
                    reader.readOptionalField(2, reader::readString, null));
            if (connectionId.isPresent() && connectionId.get().isEmpty()) {
                connectionId = Optional.empty();
            }
            long rawQueryId = reader.readRequiredField(3, reader::readUlebLong);
            Optional<Long> clientQueryId = rawQueryId == QuackConstants.OPTIONAL_INDEX_INVALID
                    ? Optional.empty() : Optional.of(rawQueryId);
            return new DecodedHeader(new MessageHeader(type, connectionId, clientQueryId));
        });
    }

    private static MessageType messageType(QuackMessage message) {
        if (message instanceof QuackMessage.ConnectionRequest) return MessageType.CONNECTION_REQUEST;
        if (message instanceof QuackMessage.ConnectionResponse) return MessageType.CONNECTION_RESPONSE;
        if (message instanceof QuackMessage.PrepareRequest) return MessageType.PREPARE_REQUEST;
        if (message instanceof QuackMessage.PrepareResponse) return MessageType.PREPARE_RESPONSE;
        if (message instanceof QuackMessage.FetchRequest) return MessageType.FETCH_REQUEST;
        if (message instanceof QuackMessage.FetchResponse) return MessageType.FETCH_RESPONSE;
        if (message instanceof QuackMessage.FetchResponseV3) return MessageType.FETCH_RESPONSE;
        if (message instanceof QuackMessage.AppendRequest) return MessageType.APPEND_REQUEST;
        // Wire id 9 was APPEND_REQUEST in v1 and is SEND_DATA_REQUEST in v3.
        if (message instanceof QuackMessage.SendDataRequest) return MessageType.APPEND_REQUEST;
        if (message instanceof QuackMessage.SendDataStreamRequest) return MessageType.APPEND_REQUEST;
        if (message instanceof QuackMessage.SendDataResponse) return MessageType.SEND_DATA_RESPONSE;
        if (message instanceof QuackMessage.FinalizeMessage) return MessageType.FINALIZE;
        if (message instanceof QuackMessage.Acknowledgement) return MessageType.ACKNOWLEDGEMENT;
        if (message instanceof QuackMessage.CancelRequest) return MessageType.CANCEL_REQUEST;
        if (message instanceof QuackMessage.HeartbeatRequest) return MessageType.HEARTBEAT_REQUEST;
        if (message instanceof QuackMessage.SuccessResponse) return MessageType.SUCCESS_RESPONSE;
        if (message instanceof QuackMessage.DisconnectMessage) return MessageType.DISCONNECT_MESSAGE;
        if (message instanceof QuackMessage.ErrorResponse) return MessageType.ERROR_RESPONSE;
        throw new QuackProtocolException("Unknown message: " + message.getClass());
    }

    private static void encodeBody(BinaryWriter writer, QuackMessage message) {
        if (message instanceof QuackMessage.ConnectionRequest m) {
            writer.writeObject(obj -> {
                writeOptionalString(obj, 1, m.authString());
                writeOptionalString(obj, 2, m.clientDuckdbVersion());
                writeOptionalString(obj, 3, m.clientPlatform());
                writeOptionalIndexDefaultZero(obj, 4, m.minSupportedQuackVersion());
                writeOptionalIndexDefaultZero(obj, 5, m.maxSupportedQuackVersion());
                writeOptionalString(obj, 6, m.clientId());
                writeOptionalIndexDefaultZero(obj, 7, m.heartbeatTimeoutSeconds());
            });
            return;
        }
        if (message instanceof QuackMessage.ConnectionResponse m) {
            writer.writeObject(obj -> {
                writeOptionalString(obj, 1, m.serverDuckdbVersion());
                writeOptionalString(obj, 2, m.serverPlatform());
                m.quackVersion().ifPresent(v -> obj.writeField(3, () -> obj.writeUleb(v)));
                writeOptionalIndexDefaultZero(obj, 4, m.heartbeatTimeoutSeconds());
            });
            return;
        }
        if (message instanceof QuackMessage.PrepareRequest m) {
            writer.writeObject(obj -> {
                if (m.sql() != null && !m.sql().isEmpty()) {
                    obj.writeField(1, () -> obj.writeString(m.sql()));
                }
                m.queryUuid().ifPresent(uuid -> obj.writeField(2, () -> obj.writeHugeInt(uuid)));
                m.inlineRows().ifPresent(rows -> obj.writeField(3, () -> obj.writeUleb(rows)));
            });
            return;
        }
        if (message instanceof QuackMessage.PrepareResponse m) {
            writer.writeObject(obj -> {
                if (!m.resultTypes().isEmpty()) {
                    obj.writeField(1, () -> obj.writeList(m.resultTypes(),
                            (t, i) -> LogicalTypeCodec.encode(obj, t)));
                }
                if (!m.resultNames().isEmpty()) {
                    obj.writeField(2, () -> obj.writeList(m.resultNames(),
                            (n, i) -> obj.writeString(n)));
                }
                if (m.needsMoreFetch()) {
                    obj.writeField(3, () -> obj.writeBool(true));
                }
                if (!m.results().isEmpty()) {
                    obj.writeField(4, () -> writeChunkPointerList(obj, m.results()));
                }
                obj.writeField(5, () -> obj.writeHugeInt(m.resultUuid()));
            });
            return;
        }
        if (message instanceof QuackMessage.FetchRequest m) {
            writer.writeObject(obj -> {
                obj.writeField(1, () -> obj.writeHugeInt(m.resultUuid()));
                writeOptionalIndexDefaultZero(obj, 2, m.batchIndex());
                writeOptionalIndexDefaultZero(obj, 3, m.ackIndex());
            });
            return;
        }
        if (message instanceof QuackMessage.FetchResponseV3 m) {
            if (m.chunkBlob()) {
                writer.writeObject(obj -> {
                    if (!m.results().isEmpty()) obj.writeField(1, () -> obj.writeUleb(m.results().size()));
                    m.totalBatches().ifPresent(total -> obj.writeField(2, () -> obj.writeUleb(total)));
                    m.batchIndex().ifPresent(index -> obj.writeField(3, () -> obj.writeUleb(index)));
                });
                for (DataChunk chunk : m.results()) VectorCodec.encodeDataChunk(writer, chunk);
                return;
            }
            writer.writeObject(obj -> {
                if (!m.results().isEmpty()) obj.writeField(1, () -> writeChunkPointerList(obj, m.results()));
                m.totalBatches().ifPresent(total -> obj.writeField(2, () -> obj.writeUleb(total)));
                obj.writeField(3, () -> {
                    obj.writeUleb(8);
                    obj.writeFixedInt64(m.batchIndex().orElse(0L));
                });
            });
            return;
        }
        if (message instanceof QuackMessage.FetchResponse m) {
            writer.writeObject(obj -> {
                if (!m.results().isEmpty()) {
                    obj.writeField(1, () -> writeChunkPointerList(obj, m.results()));
                }
                obj.writeField(2, () -> obj.writeUleb(
                        m.batchIndex().orElse(QuackConstants.OPTIONAL_INDEX_INVALID)));
            });
            return;
        }
        if (message instanceof QuackMessage.AppendRequest m) {
            writer.writeObject(obj -> {
                writeOptionalString(obj, 1, m.schemaName());
                if (m.tableName() != null && !m.tableName().isEmpty()) {
                    obj.writeField(2, () -> obj.writeString(m.tableName()));
                }
                obj.writeField(3, () -> obj.writeNullable(m.appendChunk(),
                        chunk -> VectorCodec.encodeDataChunkWrapper(obj, chunk)));
            });
            return;
        }
        if (message instanceof QuackMessage.SendDataRequest m) {
            writer.writeObject(obj -> {
                writeOptionalString(obj, 1, m.schemaName());
                obj.writeField(2, () -> obj.writeString(m.tableName()));
                obj.writeField(3, () -> writeChunkPointerList(obj, m.chunks()));
                obj.writeField(4, () -> obj.writeHugeInt(m.queryUuid()));
                obj.writeField(5, () -> obj.writeUleb(QuackConstants.OPTIONAL_INDEX_INVALID));
            });
            return;
        }
        if (message instanceof QuackMessage.SendDataStreamRequest m) {
            writer.writeObject(obj -> {
                obj.writeField(1, () -> obj.writeString(m.streamId()));
                if (!m.chunks().isEmpty()) obj.writeField(2, () -> obj.writeUleb(m.chunks().size()));
                m.totalBatches().ifPresent(total -> obj.writeField(3, () -> obj.writeUleb(total)));
                m.batchIndex().ifPresent(index -> obj.writeField(4, () -> obj.writeUleb(index)));
            });
            for (DataChunk chunk : m.chunks()) VectorCodec.encodeDataChunk(writer, chunk);
            return;
        }
        if (message instanceof QuackMessage.SendDataResponse m) {
            writer.writeObject(obj -> obj.writeField(1,
                    () -> obj.writeUleb(m.acceptBudget().orElse(QuackConstants.OPTIONAL_INDEX_INVALID))));
            return;
        }
        HugeIntParts queryUuid = message instanceof QuackMessage.FinalizeMessage m ? m.queryUuid()
                : message instanceof QuackMessage.Acknowledgement m ? m.queryUuid()
                : message instanceof QuackMessage.CancelRequest m ? m.queryUuid() : null;
        if (queryUuid != null) {
            writer.writeObject(obj -> obj.writeField(1, () -> obj.writeHugeInt(queryUuid)));
            return;
        }
        if (message instanceof QuackMessage.SuccessResponse
                || message instanceof QuackMessage.DisconnectMessage
                || message instanceof QuackMessage.HeartbeatRequest) {
            writer.writeObject(obj -> {});
            return;
        }
        if (message instanceof QuackMessage.ErrorResponse m) {
            writer.writeObject(obj -> {
                if (m.message() != null && !m.message().isEmpty()) {
                    obj.writeField(1, () -> obj.writeString(m.message()));
                }
                writeOptionalString(obj, 2, m.exceptionType());
                if (!m.extraInfo().isEmpty()) obj.writeField(3, () -> writeStringMap(obj, m.extraInfo()));
                if (m.mustInvalidate()) obj.writeField(4, () -> obj.writeBool(true));
            });
            return;
        }
        throw new QuackProtocolException("Cannot encode unsupported message type "
                + message.getClass().getSimpleName());
    }

    private static QuackMessage decodeBody(BinaryReader reader, DecodedHeader decoded, long protocolVersion) {
        MessageHeader header = decoded.header();
        return switch (header.type()) {
            case CONNECTION_REQUEST -> reader.readObject(() -> new QuackMessage.ConnectionRequest(
                    header,
                    optionalString(reader, 1),
                    optionalString(reader, 2),
                    optionalString(reader, 3),
                    Optional.of(reader.readOptionalField(4, reader::readUlebLong, 0L)),
                    Optional.of(reader.readOptionalField(5, reader::readUlebLong, 0L)),
                    optionalString(reader, 6),
                    Optional.ofNullable(reader.readOptionalField(7, reader::readUlebLong, null))));
            case CONNECTION_RESPONSE -> reader.readObject(() -> new QuackMessage.ConnectionResponse(
                    header,
                    optionalString(reader, 1),
                    optionalString(reader, 2),
                    Optional.ofNullable(reader.readOptionalField(3, reader::readUlebLong, null)),
                    Optional.ofNullable(reader.readOptionalField(4, reader::readUlebLong, null))));
            case PREPARE_REQUEST -> reader.readObject(() -> new QuackMessage.PrepareRequest(
                    header,
                    reader.readOptionalField(1, reader::readString, ""),
                    Optional.ofNullable(reader.readOptionalField(2, reader::readHugeInt, null)),
                    optionalIndex(reader.readOptionalField(3, reader::readUlebLong, -1L))));
            case PREPARE_RESPONSE -> reader.readObject(() -> {
                List<LogicalType> resultTypes = reader.readOptionalField(1,
                        () -> reader.readList(i -> LogicalTypeCodec.decode(reader)), List.of());
                List<String> resultNames = reader.readOptionalField(2,
                        () -> reader.readList(i -> reader.readString()), List.of());
                boolean needsMoreFetch = reader.readOptionalField(3, reader::readBool, false);
                List<DataChunk> results = reader.readOptionalField(4,
                        () -> readChunkPointerList(reader), List.of());
                HugeIntParts resultUuid = reader.readRequiredField(5, reader::readHugeInt);
                return new QuackMessage.PrepareResponse(header, resultTypes, resultNames,
                        needsMoreFetch, results, resultUuid);
            });
            case FETCH_REQUEST -> reader.readObject(() -> new QuackMessage.FetchRequest(
                    header,
                    reader.readRequiredField(1, reader::readHugeInt),
                    Optional.ofNullable(reader.readOptionalField(2, reader::readUlebLong, null)),
                    Optional.ofNullable(reader.readOptionalField(3, reader::readUlebLong, null))));
            case FETCH_RESPONSE -> reader.readObject(() -> {
                List<DataChunk> results = reader.readOptionalField(1,
                        () -> readChunkPointerList(reader), List.of());
                if (protocolVersion == 3) {
                    Optional<Long> total = optionalIndex(reader.readOptionalField(2, reader::readUlebLong, -1L));
                    long fixedIndex = reader.readRequiredField(3, () -> {
                        int length = reader.readUlebInt();
                        if (length != 8) throw new QuackProtocolException("Invalid fixed batch index length " + length);
                        return reader.readFixedInt64();
                    });
                    if (fixedIndex < 0) throw new QuackProtocolException("Invalid or unstamped fetch batch index");
                    Optional<Long> index = fixedIndex == 0 ? Optional.empty() : Optional.of(fixedIndex);
                    return new QuackMessage.FetchResponseV3(header, results, total, index);
                }
                long batchIndex = reader.readRequiredField(2, reader::readUlebLong);
                Optional<Long> batchOpt = batchIndex == QuackConstants.OPTIONAL_INDEX_INVALID
                        ? Optional.empty() : Optional.of(batchIndex);
                return new QuackMessage.FetchResponse(header, results, batchOpt);
            });
            case APPEND_REQUEST -> reader.readObject(() -> {
                Optional<String> schemaName = optionalString(reader, 1);
                String tableName = reader.readOptionalField(2, reader::readString, "");
                if (protocolVersion == 3) {
                    List<DataChunk> chunks = reader.readOptionalField(3, () -> readChunkPointerList(reader), List.of());
                    HugeIntParts uuid = reader.readRequiredField(4, reader::readHugeInt);
                    long index = reader.readRequiredField(5, reader::readUlebLong);
                    if (index != QuackConstants.OPTIONAL_INDEX_INVALID) {
                        throw new QuackProtocolException("Only unordered SEND_DATA decoding is supported");
                    }
                    return new QuackMessage.SendDataRequest(header, schemaName, tableName, chunks, uuid);
                }
                DataChunk chunk = reader.readOptionalField(3,
                        () -> reader.readNullable(() -> VectorCodec.decodeDataChunkWrapper(reader)),
                        null);
                if (chunk == null) {
                    throw new QuackProtocolException("APPEND_REQUEST is missing append_chunk");
                }
                return new QuackMessage.AppendRequest(header, schemaName, tableName, chunk);
            });
            case SUCCESS_RESPONSE -> reader.readObject(() -> new QuackMessage.SuccessResponse(header));
            case DISCONNECT_MESSAGE -> reader.readObject(() -> new QuackMessage.DisconnectMessage(header));
            case HEARTBEAT_REQUEST -> reader.readObject(() -> new QuackMessage.HeartbeatRequest(header));
            case SEND_DATA_RESPONSE -> reader.readObject(() -> new QuackMessage.SendDataResponse(header,
                    optionalIndex(reader.readRequiredField(1, reader::readUlebLong))));
            case FINALIZE -> reader.readObject(() -> new QuackMessage.FinalizeMessage(header,
                    reader.readRequiredField(1, reader::readHugeInt)));
            case ACKNOWLEDGEMENT -> reader.readObject(() -> new QuackMessage.Acknowledgement(header,
                    reader.readOptionalField(1, reader::readHugeInt, new HugeIntParts(0, 0))));
            case CANCEL_REQUEST -> reader.readObject(() -> new QuackMessage.CancelRequest(header,
                    reader.readRequiredField(1, reader::readHugeInt)));
            case ERROR_RESPONSE -> reader.readObject(() -> new QuackMessage.ErrorResponse(
                    header,
                    reader.readOptionalField(1, reader::readString, ""),
                    optionalString(reader, 2),
                    reader.readOptionalField(3, () -> readStringMap(reader), Map.of()),
                    reader.readOptionalField(4, reader::readBool, false)));
            case INVALID -> throw new QuackProtocolException(
                    "Cannot decode message type " + header.type());
        };
    }

    private static QuackMessage.FetchResponseV3 decodeChunkBlobFetch(BinaryReader reader, MessageHeader header) {
        int chunkCount = reader.readOptionalField(1, reader::readUlebInt, 0);
        Optional<Long> total = optionalIndex(reader.readOptionalField(2, reader::readUlebLong, -1L));
        Optional<Long> index = optionalIndex(reader.readOptionalField(3, reader::readUlebLong, -1L));
        reader.readEndObject();
        if (index.isPresent() && index.get() <= 0 || total.isPresent() && total.get() < 0) {
            throw new QuackProtocolException("Invalid Quack fetch batch index or total");
        }
        List<DataChunk> chunks = new ArrayList<>(Math.min(chunkCount, 1024));
        for (int i = 0; i < chunkCount; i++) chunks.add(VectorCodec.decodeDataChunk(reader));
        return new QuackMessage.FetchResponseV3(header, chunks, total, index, true);
    }

    private static QuackMessage.SendDataStreamRequest decodeChunkBlobSend(BinaryReader reader, MessageHeader header) {
        String streamId = reader.readRequiredField(1, reader::readString);
        int chunkCount = reader.readOptionalField(2, reader::readUlebInt, 0);
        Optional<Long> total = optionalIndex(reader.readOptionalField(3, reader::readUlebLong, -1L));
        Optional<Long> index = optionalIndex(reader.readOptionalField(4, reader::readUlebLong, -1L));
        reader.readEndObject();
        if (streamId.isEmpty() || index.isPresent() && index.get() <= 0
                || total.isPresent() && total.get() < 0) {
            throw new QuackProtocolException("Invalid Quack send stream or batch index");
        }
        List<DataChunk> chunks = new ArrayList<>(Math.min(chunkCount, 1024));
        for (int i = 0; i < chunkCount; i++) chunks.add(VectorCodec.decodeDataChunk(reader));
        return new QuackMessage.SendDataStreamRequest(header, streamId, chunks, total, index);
    }

    private static Map<String, String> readStringMap(BinaryReader reader) {
        int count = reader.readUlebInt();
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) reader.readObject(() -> {
            String key = reader.readRequiredField(0, reader::readString);
            String value = reader.readRequiredField(1, reader::readString);
            if (result.putIfAbsent(key, value) != null) {
                throw new QuackProtocolException("Duplicate error detail " + key);
            }
            return null;
        });
        return Map.copyOf(result);
    }

    private static void writeStringMap(BinaryWriter writer, Map<String, String> values) {
        writer.writeUleb(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) writer.writeObject(obj -> {
            obj.writeField(0, () -> obj.writeString(entry.getKey()));
            obj.writeField(1, () -> obj.writeString(entry.getValue()));
        });
    }

    private static Optional<String> optionalString(BinaryReader reader, int fieldId) {
        return Optional.ofNullable(reader.readOptionalField(fieldId, reader::readString, null));
    }

    private static Optional<Long> optionalIndex(long value) {
        return value == QuackConstants.OPTIONAL_INDEX_INVALID ? Optional.empty() : Optional.of(value);
    }

    private static void writeOptionalString(BinaryWriter writer, int fieldId, Optional<String> value) {
        value.filter(s -> !s.isEmpty()).ifPresent(v ->
                writer.writeField(fieldId, () -> writer.writeString(v)));
    }

    private static void writeOptionalIndexDefaultZero(BinaryWriter writer, int fieldId, Optional<Long> value) {
        value.filter(v -> v != 0L).ifPresent(v ->
                writer.writeField(fieldId, () -> writer.writeUleb(v)));
    }

    private static List<DataChunk> readChunkPointerList(BinaryReader reader) {
        return reader.readList(i -> {
            DataChunk chunk = reader.readNullable(() -> VectorCodec.decodeDataChunkWrapper(reader));
            if (chunk == null) {
                throw new QuackProtocolException("Encountered null DataChunk pointer in result list");
            }
            return chunk;
        });
    }

    private static void writeChunkPointerList(BinaryWriter writer, List<DataChunk> chunks) {
        writer.writeList(chunks, (chunk, i) -> writer.writeNullable(chunk,
                value -> VectorCodec.encodeDataChunkWrapper(writer, value)));
    }

    private record DecodedHeader(MessageHeader header) {}

    /** Just so unused warnings stay quiet during early iteration. */
    @SuppressWarnings("unused")
    private static final List<Object> UNUSED = new ArrayList<>();
}
