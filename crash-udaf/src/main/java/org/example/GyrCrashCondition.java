package org.example;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
        name = "gyr_crash_condition",
        description = "1. check if it is above the threshold\n",
        author = "hyunjune",
        version = "0.0.1")
public class GyrCrashCondition {

    private static final String CURRENT = "CURRENT";
    private static final Double THRESHOLD = 10.0;

    @UdafFactory(description = "check if a crash has occurred.",
    aggregateSchema = "STRUCT<MAX double, STATUS boolean>")
    public static Udaf<Double, Struct, Boolean> createUdaf(){

        final Schema CHECK = SchemaBuilder.struct().optional()
                .field(CURRENT, Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        return new TableUdaf<Double, Struct, Boolean>()
        {
            @Override
            public Struct initialize() {
                return new Struct(CHECK).put(CURRENT, 0.0);
            }

            @Override
            public Struct aggregate(Double current, Struct aggregate) {
                if (current == null) {
                    return aggregate;
                }
                return new Struct(CHECK).put(CURRENT, current);
            }

            @Override
            public Struct merge(Struct aggOne, Struct aggTwo) {
                return aggOne;
            }

            @Override
            public Boolean map(Struct aggregate) {
                return aggregate.getFloat64(CURRENT) >= THRESHOLD;
            }

            @Override
            public Struct undo(Double valueToUndo, Struct aggregateValue) {
                return null;
            }
        };
    }
}
