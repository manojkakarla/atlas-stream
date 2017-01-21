package com.atlas.stream.stream.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

public abstract class AtlasTopology {

    protected final TopologyFactory topologyFactory;
    protected final String name;
    protected final Config conf;

    public AtlasTopology(String name, TopologyFactory topologyFactory, Config conf) {
        this.topologyFactory = topologyFactory;
        this.name = name;
        this.conf = conf;
    }

    public void start() throws Exception {
        StormTopology topology = topologyFactory.getTopology();
        StormSubmitter.submitTopology(name, conf, topology);
    }
}
