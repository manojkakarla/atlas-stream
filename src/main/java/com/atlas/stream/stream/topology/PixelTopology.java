package com.atlas.stream.stream.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PixelTopology extends AtlasTopology {


    public PixelTopology(String name, TopologyFactory topologyFactory, Config conf) {
        super(name, topologyFactory, conf);
    }

    @Override
    public void start() throws Exception {
        log.info("------------------------------------------");
        log.info("        Starting cluster topology");
        log.info("------------------------------------------");
        StormSubmitter.submitTopology(name, conf, topologyFactory.getTopology());
    }
}
