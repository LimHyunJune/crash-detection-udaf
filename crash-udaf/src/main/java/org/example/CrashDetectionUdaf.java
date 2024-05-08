package org.example;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
        name = "crash-detection",
        description = "check 3 conditions\n" +
                "1. gyr >= threshold?\n" +
                "2. acc >= threshold?\n" +
                "3. Does acc continue to increase when groupby is done with gyr?\n",
        author = "hyunjune",
        version = "0.0.1")
public class CrashDetectionUdaf{

    private static final String MAX = "MAX";
    private static final String STATUS = "STATUS";

    private static final Double THRESHOLD = 0.1;

    @UdafFactory(description = "check if a crash has occurred.",
    aggregateSchema = "STRUCT<MAX double, STATUS boolean>")
    public static Udaf<Double, Struct, Boolean> createUdaf(){

        final Schema CHECK = SchemaBuilder.struct().optional()
                .field(MAX, Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field(STATUS, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
        return new TableUdaf<Double, Struct, Boolean>()
        {
            @Override
            public Struct initialize() {
                return new Struct(CHECK).put(MAX, 0.0).put(STATUS, true);
            }

            @Override
            public Struct aggregate(Double current, Struct aggregate) {
                if (current == null) {
                    return aggregate;
                }

                if(!aggregate.getBoolean(STATUS))
                    return aggregate;

                if(aggregate.getFloat64(MAX) > current || current < THRESHOLD)
                {
                    return new Struct(CHECK).put(STATUS, false);
                }

                return new Struct(CHECK).put(MAX, current);
            }

            @Override
            public Struct merge(Struct aggOne, Struct aggTwo) {
                return aggOne;
            }

            @Override
            public Boolean map(Struct aggregate) {
                return aggregate.getBoolean(STATUS);
            }

            @Override
            public Struct undo(Double valueToUndo, Struct aggregateValue) {
                return null;
            }
        };
    }
}
