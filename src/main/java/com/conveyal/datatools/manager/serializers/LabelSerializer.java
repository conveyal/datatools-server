package com.conveyal.datatools.manager.serializers;

import com.conveyal.datatools.manager.controllers.api.LabelController;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.sql.Array;
import java.util.ArrayList;

public class LabelSerializer extends StdSerializer<ArrayList<String>> {

    protected LabelSerializer() {
        super((Class<ArrayList<String>>) new ArrayList<String>().getClass());
    }

    protected LabelSerializer(Class<ArrayList<String>> t) {
        super(t);
    }

    protected LabelSerializer(JavaType type) {
        super(type);
    }

    protected LabelSerializer(Class<?> t, boolean dummy) {
        super(t, dummy);
    }

    protected LabelSerializer(StdSerializer<?> src) {
        super(src);
    }

    @Override
    public void serialize(ArrayList<String> strings, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartArray();
        for (String labelId: strings) {
            jsonGenerator.writeStartObject();

            Label label = Persistence.labels.getById(labelId);
            jsonGenerator.writeStringField("id", label.id);
            jsonGenerator.writeStringField("projectId", label.projectId);
            jsonGenerator.writeStringField("name", label.name);
            jsonGenerator.writeStringField("description", label.description);
            jsonGenerator.writeStringField("color", label.color);
            jsonGenerator.writeBooleanField("adminOnly", label.adminOnly);

            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }
}