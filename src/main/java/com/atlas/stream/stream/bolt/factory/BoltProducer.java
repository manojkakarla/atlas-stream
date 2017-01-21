package com.atlas.stream.stream.bolt.factory;

import backtype.storm.topology.IRichBolt;

public abstract class BoltProducer<B extends IRichBolt> {

    private final Class<B> boltClass;

    public BoltProducer(Class<B> boltClass) {
        this.boltClass = boltClass;

    }

    public abstract B create();

    public Class<B> getBoltClass() {
        return boltClass;
    }
}
