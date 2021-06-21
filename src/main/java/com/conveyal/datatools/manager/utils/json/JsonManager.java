package com.conveyal.datatools.manager.utils.json;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;

import com.conveyal.gtfs.util.json.JacksonSerializers;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for writing REST API routines
 * @author mattwigway
 *
 */
public class JsonManager<T> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonManager.class);
    private ObjectWriter ow;
    private ObjectMapper om;

    /**
     * Create a new JsonManager
     * @param theClass The class to create a json manager for (yes, also in the diamonds).
     * @param view The view to use
     */
    public JsonManager (Class<T> theClass, Class view) {
        this.theClass = theClass;
        this.om = new ObjectMapper();
        // previous model for gtfs validation errors
//        om.addMixIn(InvalidValue.class, InvalidValueMixIn.class);
        // TODO: Removes extraneous mixins? These may be needed to import data from MapDB-backed versions of this
        //  software to the MongoDB-backed system.
        om.addMixIn(Rectangle2D.class, Rectangle2DMixIn.class);
        SimpleModule deser = new SimpleModule();

        deser.addDeserializer(LocalDate.class, new LocalDateDeserializer());
        deser.addSerializer(LocalDate.class, new LocalDateSerializer());

        deser.addDeserializer(Rectangle2D.class, new Rectangle2DDeserializer());
        om.registerModule(deser);
        om.getSerializerProvider().setNullKeySerializer(new JacksonSerializers.MyDtoNullKeySerializer());
//        om.registerModule(new JavaTimeModule());
        SimpleFilterProvider filters = new SimpleFilterProvider();
        filters.addFilter("bbox", SimpleBeanPropertyFilter.filterOutAllExcept("west", "east", "south", "north"));
        this.ow = om.writer(filters).withView(view);
    }

    private Class<T> theClass;

    /**
     * Add an additional mixin for serialization with this object mapper.
     */
    public void addMixin(Class target, Class mixin) {
        om.addMixIn(target, mixin);
    }

    /**
     * Convert an object to its JSON representation or, if object is a string, simply cast and return the string.
     * @param o the object to convert
     * @return the JSON string
     * @throws JsonProcessingException
     */
    public String write(Object o) throws JsonProcessingException{
        if (o instanceof String) {
            return (String) o;
        }
        return ow.writeValueAsString(o);
    }

    /**
     * Convert a collection of objects to their JSON representation.
     * @param c the collection
     * @return A JsonNode representing the collection
     * @throws JsonProcessingException
     */
    public String write (Collection<T> c) throws JsonProcessingException {
        return ow.writeValueAsString(c);
    }

    public String write (Map<String, T> map) throws JsonProcessingException {
        return ow.writeValueAsString(map);
    }

    public T read (String s) throws JsonParseException, JsonMappingException, IOException {
        return om.readValue(s, theClass);
    }

    public T read (JsonParser p) throws JsonParseException, JsonMappingException, IOException {
        return om.readValue(p, theClass);
    }

    public T read(JsonNode asJson) {
        return om.convertValue(asJson, theClass);
    }

    /** serialize local dates as noon GMT epoch times */
    public static class LocalDateSerializer extends StdScalarSerializer<LocalDate> {
        private static final long serialVersionUID = 3153194744968260324L;

        public LocalDateSerializer() {
            super(LocalDate.class, false);
        }

        @Override
        public void serialize(LocalDate ld, JsonGenerator jgen, SerializerProvider arg2) throws IOException {
            // YYYYMMDD
            jgen.writeString(ld.format(DateTimeFormatter.BASIC_ISO_DATE));
        }
    }

    /** deserialize local dates from GMT epochs */
    public static class LocalDateDeserializer extends StdScalarDeserializer<LocalDate> {
        private static final long serialVersionUID = -1855560624079270379L;

        public LocalDateDeserializer () {
            super(LocalDate.class);
        }

        @Override
        public LocalDate deserialize(JsonParser jp, DeserializationContext arg1) throws IOException {
            String dateText = jp.getText();
            try {
                return LocalDate.parse(dateText, DateTimeFormatter.BASIC_ISO_DATE);
            } catch (Exception jsonException) {
                // This is here to catch any loads of database dumps that happen to have the old java.util.Date
                // field type in validationResult.  God help us.
                LOG.warn("Error parsing date value: `{}`, trying legacy java.util.Date date format", dateText);
                try {
                    return Instant.ofEpochMilli(jp.getValueAsLong()).atZone(ZoneOffset.UTC).toLocalDate();
                } catch (Exception e) {
                    LOG.warn("Error parsing date value: `{}`", dateText);
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

}
