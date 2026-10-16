package com.gizmodata.quack.jdbc;

import java.util.Map;
import java.util.Optional;

public class QuackServerException extends QuackException {
    private final String serverMessage;
    private final Optional<String> exceptionType;
    private final Map<String, String> extraInfo;
    private final boolean mustInvalidate;

    public QuackServerException(String serverMessage) {
        this(serverMessage, Optional.empty(), Map.of(), false);
    }

    public QuackServerException(String serverMessage, Optional<String> exceptionType,
                                Map<String, String> extraInfo, boolean mustInvalidate) {
        super(serverMessage);
        this.serverMessage = serverMessage;
        this.exceptionType = exceptionType;
        this.extraInfo = Map.copyOf(extraInfo);
        this.mustInvalidate = mustInvalidate;
    }

    public String getServerMessage() {
        return serverMessage;
    }

    public Optional<String> getExceptionType() { return exceptionType; }
    public Map<String, String> getExtraInfo() { return extraInfo; }
    public boolean mustInvalidate() { return mustInvalidate; }
}
