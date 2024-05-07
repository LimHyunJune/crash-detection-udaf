package org.example;

import io.confluent.ksql.function.udaf.UdafDescription;

@UdafDescription(
        name = "crash-detection",
        description = "check 3 conditions\n" +
                "1. gyr >= threshold?\n" +
                "2. acc >= threshold?\n" +
                "3. Does acc continue to increase when groupby is done with gyr?\n",
        author = "hyunjune",
        version = "0.0.1")
public class CrashDetectionUdaf{
}
