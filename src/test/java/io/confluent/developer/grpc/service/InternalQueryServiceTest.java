package io.confluent.developer.grpc.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.developer.model.StockTransactionAggregation;
import io.confluent.developer.proto.HostInfoProto;
import io.confluent.developer.proto.KeyQueryMetadataProto;
import io.confluent.developer.proto.KeyQueryRequestProto;
import io.confluent.developer.proto.QueryResponseProto;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import io.confluent.developer.streams.SerdeUtil;
import io.grpc.internal.testing.StreamRecorder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class InternalQueryServiceTest {

    private final KafkaStreams kafkaStreams = Mockito.mock(KafkaStreams.class);
    private InternalQueryService internalQueryService;
    private final String storeName = "custom-store";

    private final ObjectWriter objectWriter = new ObjectMapper().writer();
    @BeforeEach
    void setUp() {
        internalQueryService = new InternalQueryService(kafkaStreams);
        internalQueryService.stringSerde = Serdes.String();
        internalQueryService.storeName = storeName;
    }

    @Test()
    @DisplayName("Testing for single key query")
    void keyQueryServiceTest() throws Exception {
        HostInfoProto hostInfoProto = HostInfoProto.newBuilder()
                .setHost("localhost")
                .setPort(5059)
                .build();
        KeyQueryMetadataProto keyQueryMetadataProto = KeyQueryMetadataProto.newBuilder()
                .setPartition(0)
                .setActiveHost(hostInfoProto)
                .build();
        KeyQueryRequestProto queryRequestProto = KeyQueryRequestProto.newBuilder()
                .setSymbol("CFLT")
                .setKeyQueryMetadata(keyQueryMetadataProto)
                .build();

        long currentTime = Instant.now().toEpochMilli();


        StockTransactionAggregationProto.Builder expectedProto = StockTransactionAggregationProto.newBuilder()
                .setSymbol("CFLT")
                .setSells(1000.0)
                .setBuys(1000.0);

        StockTransactionAggregation aggregation = new StockTransactionAggregation("CFLT", 1000.0, 1000.0);


        QueryResult<StockTransactionAggregation> queryResult = QueryResult.forResult(aggregation);
        StateQueryResult<StockTransactionAggregation> stateQueryResult = new StateQueryResult<>();
        stateQueryResult.addResult(1, queryResult);

        when(kafkaStreams.query(Mockito.any(StateQueryRequest.class))).thenReturn(stateQueryResult);


        StreamRecorder<QueryResponseProto> responseObserver = StreamRecorder.create();
        internalQueryService.keyQueryService(queryRequestProto, responseObserver);
        if (!responseObserver.awaitCompletion(5, TimeUnit.SECONDS)) {
            fail("The call did not terminate in time");
        }
        assertNull(responseObserver.getError());
        QueryResponseProto queryResponseProto = responseObserver.getValues().get(0);
        assertEquals(queryResponseProto.getAggregations(0), expectedProto.build());


    }

    @Test
    void rangeQueryServiceTest() {
    }

    @Test
    void mulitKeyQueryServiceTest() {
    }
}