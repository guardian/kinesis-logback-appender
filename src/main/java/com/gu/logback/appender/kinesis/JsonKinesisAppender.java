package com.gu.logback.appender.kinesis;

import ch.qos.logback.core.spi.DeferredProcessingAware;

import java.util.Iterator;
import java.util.Map;
import javax.json.*;

public class JsonKinesisAppender <Event extends DeferredProcessingAware>
        extends KinesisAppender<Event> {

    public JsonKinesisAppender(Map<String, String> standardTags) {
        super();
        this.standardTags = standardTags;
    }

    private Map<String, String> standardTags;

    @Override
    protected void putMessage(String message) throws Exception {
        // build a json object
        JsonObjectBuilder json = Json.createObjectBuilder();
        Iterator i = standardTags.keySet().iterator();
        while (i.hasNext()) {
            String k = (String) i.next();
            String v = standardTags.get(k);
            json.add(k, v);
        }
        json.add("message", message);
        super.putMessage(json.build().toString());
    }

}
