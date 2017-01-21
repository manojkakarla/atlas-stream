package com.atlas.stream.stream.bolt.factory;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.IRichBolt;

public class BoltFactory {

    private Map<Class, BoltProducer<? extends IRichBolt>> producers = new HashMap<>();

    public <B extends IRichBolt> BoltProducer<B> boltProducer(Class<B> clazz) {
        return (BoltProducer<B>) producers.get(clazz);
    }

    public <B extends IRichBolt> void registerProducer(BoltProducer<B> producer) {
        producers.put(producer.getBoltClass(), producer);
    }
}
