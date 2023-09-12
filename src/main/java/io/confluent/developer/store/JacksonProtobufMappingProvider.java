package io.confluent.developer.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import io.confluent.developer.proto.StockTransactionAggregationProto;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JacksonProtobufMappingProvider implements MappingProvider {
     private final StockTransactionAggregationProto.Builder builder = StockTransactionAggregationProto.newBuilder();
     private final JsonFormat.Parser parser = JsonFormat.parser();
    private final  TypeRef<List<KeyValue<String, StockTransactionAggregationProto>>> expectedTypeRef = new TypeRef<>() {};
    @Override
    public <T> T map(Object source, Class<T> targetType, Configuration configuration) {
       throw new IllegalStateException("not supported");
    }

    @Override
    public <T> T map(Object source, TypeRef<T> targetType, Configuration configuration) {
        Objects.requireNonNull(targetType, "TypeRef can't be null");
         if(!Objects.equals(targetType.getType().getTypeName(), expectedTypeRef.getType().getTypeName())) {
             throw new IllegalArgumentException(String.format("Expected TypeRef of [%s] but was [%s]",
                     expectedTypeRef.getType().getTypeName(), targetType.getType().getTypeName()));
        }

        ArrayNode arrayNode = (ArrayNode)source;
        ArrayList<KeyValue<String, StockTransactionAggregationProto>> aggregations = new ArrayList<>();
        arrayNode.elements().forEachRemaining(jsonNode -> {
             StockTransactionAggregationProto agg = fromJsonNode(jsonNode, builder);
             aggregations.add(KeyValue.pair(agg.getSymbol(), agg));
        });
        return (T) aggregations;
    }


    private StockTransactionAggregationProto fromJsonNode(final JsonNode jsonNode, final StockTransactionAggregationProto.Builder builder) {
        try {
            parser.merge(jsonNode.toString(), builder);
            StockTransactionAggregationProto aggregationProto = builder.build();
            builder.clear();
            return aggregationProto;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
