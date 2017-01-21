package com.atlas.stream.stream.bolt.factory;

import com.atlas.client.JsonConverter;
import com.atlas.stream.stream.bolt.EventProcessingBolt;

public class EventProcessingBoltProducer extends BoltProducer<EventProcessingBolt> {


    private final JsonConverter converter;

    public EventProcessingBoltProducer(JsonConverter converter) {
        super(EventProcessingBolt.class);
        this.converter = converter;
    }

    @Override
    public EventProcessingBolt create() {
         return new EventProcessingBolt(converter);
    }
}
