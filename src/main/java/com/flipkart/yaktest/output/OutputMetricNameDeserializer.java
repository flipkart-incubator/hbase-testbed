package com.flipkart.yaktest.output;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class OutputMetricNameDeserializer extends StdDeserializer<OutputMetricName> {

    protected OutputMetricNameDeserializer() {
        super(OutputMetricName.class);
    }

    @Override
    public OutputMetricName deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        OutputMetricName outputMetricName;
        try {

            outputMetricName = HbaseOutputMetricName.valueOf(jsonParser.getText());
        } catch (Exception e) {
            outputMetricName = SepOutputMetricName.valueOf(jsonParser.getText());
        }
        return outputMetricName;
    }
}
