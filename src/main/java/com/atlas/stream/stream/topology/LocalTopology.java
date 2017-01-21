package com.atlas.stream.stream.topology;

import java.util.concurrent.CountDownLatch;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalTopology extends AtlasTopology {

    private final CountDownLatch latch;

    public LocalTopology(String name, TopologyFactory topologyFactory, Config conf, CountDownLatch latch) {
        super(name, topologyFactory, conf);
        this.latch = latch;
    }

    @Override
    public void start() throws Exception {
        log.info("------------------------------------------");
        log.info("        Starting local topology");
        log.info("------------------------------------------");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, topologyFactory.getTopology());
        latch.await();
        cluster.shutdown();
    }
}
