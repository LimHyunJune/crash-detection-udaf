package org.example;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.checkerframework.checker.units.qual.C;

@UdafDescription(
        name = "acc_crash_condition",
        description = "1. check for 8 increase in acceleration\n" +
                      "2. check if it is above the threshold\n",
        author = "hyunjune",
        version = "0.0.2")
public class AccCrashCondition {

    private static final String CURRENT = "CURRENT";
    private static final String COUNT = "COUNT";
    private static final String CRASH = "CRASH";

    private static final Double THRESHOLD = 13.0;

    @UdafFactory(description = "check if a crash has occurred.",
    aggregateSchema = "STRUCT<CURRENT double, COUNT integer, CRASH boolean>")
    public static Udaf<Double, Struct, Boolean> createUdaf(){

        final Schema CHECK = SchemaBuilder.struct().optional()
                .field(CURRENT, Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field(COUNT, Schema.OPTIONAL_INT32_SCHEMA)
                .field(CRASH, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
        return new TableUdaf<Double, Struct, Boolean>()
        {
            @Override
            public Struct initialize() {
                return new Struct(CHECK).put(CURRENT, 0.0).put(COUNT, 0).put(CRASH, false);
            }

            @Override
            public Struct aggregate(Double current, Struct aggregate) {
                if (current == null) {
                    return aggregate;
                }

                if(aggregate.getBoolean(CRASH))
                    return aggregate;

                double before = aggregate.getFloat64(CURRENT);
                int count = aggregate.getInt32(COUNT);

                if(before < current)
                {
                    count++;
                    if(count >= 8)
                    {
                        if(current >= THRESHOLD)
                            return new Struct(CHECK).put(CURRENT, current).put(COUNT, count).put(CRASH, true);
                        count = 0;
                    }
                }
                else
                    count = 0;

                return new Struct(CHECK).put(CURRENT, current).put(COUNT, count).put(CRASH, false);
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
