package com.gizmodata.quack.jdbc.message;

import com.gizmodata.quack.jdbc.codec.HugeIntParts;
import com.gizmodata.quack.jdbc.type.LogicalType;

import java.util.List;
import java.util.Optional;

public sealed interface QuackMessage {

    MessageHeader header();

    record ConnectionRequest(MessageHeader header,
                             Optional<String> authString,
                             Optional<String> clientDuckdbVersion,
                             Optional<String> clientPlatform,
                             Optional<Long> minSupportedQuackVersion,
                             Optional<Long> maxSupportedQuackVersion,
                             Optional<String> clientId,
                             Optional<Long> heartbeatTimeoutSeconds) implements QuackMessage {
        public ConnectionRequest(MessageHeader header, Optional<String> authString,
                                 Optional<String> clientDuckdbVersion, Optional<String> clientPlatform,
                                 Optional<Long> minSupportedQuackVersion, Optional<Long> maxSupportedQuackVersion) {
            this(header, authString, clientDuckdbVersion, clientPlatform, minSupportedQuackVersion,
                    maxSupportedQuackVersion, Optional.empty(), Optional.empty());
        }
    }

    record ConnectionResponse(MessageHeader header,
                              Optional<String> serverDuckdbVersion,
                              Optional<String> serverPlatform,
                              Optional<Long> quackVersion,
                              Optional<Long> heartbeatTimeoutSeconds) implements QuackMessage {
        public ConnectionResponse(MessageHeader header, Optional<String> serverDuckdbVersion,
                                  Optional<String> serverPlatform, Optional<Long> quackVersion) {
            this(header, serverDuckdbVersion, serverPlatform, quackVersion, Optional.empty());
        }
    }

    record PrepareRequest(MessageHeader header, String sql, Optional<HugeIntParts> queryUuid) implements QuackMessage {
        public PrepareRequest(MessageHeader header, String sql) {
            this(header, sql, Optional.empty());
        }
    }

    record PrepareResponse(MessageHeader header,
                           List<LogicalType> resultTypes,
                           List<String> resultNames,
                           boolean needsMoreFetch,
                           List<DataChunk> results,
                           HugeIntParts resultUuid) implements QuackMessage {}

    record FetchRequest(MessageHeader header, HugeIntParts resultUuid,
                        Optional<Long> batchIndex, Optional<Long> ackIndex) implements QuackMessage {
        public FetchRequest(MessageHeader header, HugeIntParts resultUuid) {
            this(header, resultUuid, Optional.empty(), Optional.empty());
        }
    }

    record FetchResponse(MessageHeader header,
                         List<DataChunk> results,
                         Optional<Long> batchIndex) implements QuackMessage {}

    /** V3 uses field 2 for the terminal total and field 3 for a fixed-width batch index. */
    record FetchResponseV3(MessageHeader header, List<DataChunk> results,
                           Optional<Long> totalBatches, Optional<Long> batchIndex) implements QuackMessage {}

    record AppendRequest(MessageHeader header,
                         Optional<String> schemaName,
                         String tableName,
                         DataChunk appendChunk) implements QuackMessage {}

    /** One unordered insert batch; FINALIZE completes the statement, or DISCONNECT aborts the unfinished stream. */
    record SendDataRequest(MessageHeader header, Optional<String> schemaName, String tableName,
                           List<DataChunk> chunks, HugeIntParts queryUuid) implements QuackMessage {}

    record SendDataResponse(MessageHeader header, Optional<Long> acceptBudget) implements QuackMessage {}

    record FinalizeMessage(MessageHeader header, HugeIntParts queryUuid) implements QuackMessage {}

    record Acknowledgement(MessageHeader header, HugeIntParts queryUuid) implements QuackMessage {}

    record CancelRequest(MessageHeader header, HugeIntParts queryUuid) implements QuackMessage {}

    record HeartbeatRequest(MessageHeader header) implements QuackMessage {}

    record SuccessResponse(MessageHeader header) implements QuackMessage {}

    record DisconnectMessage(MessageHeader header) implements QuackMessage {}

    record ErrorResponse(MessageHeader header, String message) implements QuackMessage {}
}
