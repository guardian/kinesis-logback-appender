package com.gu.logback.appender.kinesis;

import ch.qos.logback.core.spi.DeferredProcessingAware;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

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
        Map<String,String> payload = new HashMap<String, String>();
        payload.putAll(standardTags);
        payload.put("message", message);
        super.putMessage(new ObjectMapper().writeValueAsString(payload));
    }

}
