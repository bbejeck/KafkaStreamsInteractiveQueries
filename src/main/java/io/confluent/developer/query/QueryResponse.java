package io.confluent.developer.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QueryResponse <T> {

    private final String errorMessage;
    private final T result;
    private Map<String, Set<String>> executionInfo;

    private QueryResponse(String exception, T result) {
        this.errorMessage = exception;
        this.result = result;
    }

    public static <T> QueryResponse<T> withError(String errorMessage) {
         return new QueryResponse<>(errorMessage, null);
    }

    public static <T> QueryResponse<T> withResult(T result) {
        return new QueryResponse<>(null, result);
    }

    public QueryResponse<T> addExecutionInfo(Map<String, Set<String>>executionInfo) {
        this.executionInfo = new HashMap<>(executionInfo);
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public T getResult() {
        return result;
    }

    public Map<String, Set<String>> getExecutionInfo() {
        return executionInfo;
    }
}
