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
        version = "0.0.2")
public class GyrCrashCondition {

    private static final String CRASH = "CRASH";
    private static final Double THRESHOLD = 10.0;

    @UdafFactory(description = "check if a crash has occurred.",
    aggregateSchema = "STRUCT<CRASH boolean>")
    public static Udaf<Double, Struct, Boolean> createUdaf(){

        final Schema CHECK = SchemaBuilder.struct().optional()
                .field(CRASH, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
        return new TableUdaf<Double, Struct, Boolean>()
        {
            @Override
            public Struct initialize() {
                return new Struct(CHECK).put(CRASH, false);
            }

            @Override
            public Struct aggregate(Double current, Struct aggregate) {
                if (current == null) {
                    return aggregate;
                }

                if(aggregate.getBoolean(CRASH))
                    return aggregate;

                if(current >= THRESHOLD)
                    return new Struct(CHECK).put(CRASH, true);

                return new Struct(CHECK).put(CRASH, false);
            }

            @Override
            public Struct merge(Struct aggOne, Struct aggTwo) {
                return aggOne;
            }

            @Override
            public Boolean map(Struct aggregate) {
                return aggregate.getBoolean(CRASH);
            }

            @Override
            public Struct undo(Double valueToUndo, Struct aggregateValue) {
                return null;
            }
        };
    }
}
