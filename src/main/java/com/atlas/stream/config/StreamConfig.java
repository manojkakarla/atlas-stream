package com.atlas.stream.config;

import backtype.storm.Config;
import com.atlas.client.JsonConverter;
import com.atlas.client.config.JsonConfig;
import com.atlas.core.MessageProducer;
import com.atlas.core.dw.AppConfig;
import com.atlas.stream.stream.bolt.factory.*;
import com.atlas.stream.stream.domain.AggregationLimits;
import com.atlas.stream.stream.domain.TransferMessage;
import com.atlas.stream.stream.topology.LocalTopology;
import com.atlas.stream.stream.topology.AtlasTopology;
import com.atlas.stream.stream.topology.PixelTopology;
import com.atlas.stream.stream.topology.TopologyFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Configuration
@Import(JsonConfig.class)
public class StreamConfig {

    @Autowired
    private AppConfig properties;
    @Autowired
    private JsonConverter converter;
    @Autowired
    private MessageProducer<TransferMessage> messageProducer;

    @Bean(initMethod = "start")
    public AtlasTopology atlasTopology() {
        String type = properties.get("type");
        Config conf = new Config();
        conf.setDebug(true);
        String name = "atlas-stream";
        AtlasTopology topology;
        if ("local".equals(type)) {
            topology = new LocalTopology(name, topologyFactory(), conf, new CountDownLatch(1));
        } else {
            conf.setNumWorkers(2);
            conf.setMaxSpoutPending(5000);
            topology = new PixelTopology(name, topologyFactory(), conf);
        }
        return topology;
    }

    @Bean
    public TopologyFactory topologyFactory() {

        BrokerHosts zk = new ZkHosts(properties.get("zk.connect"), properties.get("zk.storm.root"));

        SpoutConfig spoutConf = getSpoutConfig(zk, "topic", "streamConsumer");
        SpoutConfig cmdConf = getSpoutConfig(zk, "cmd_topic", "cmdConsumer");

        return new TopologyFactory(spoutConf, cmdConf, boltFactory());
    }

    @Bean
    public BoltFactory boltFactory() {
        BoltFactory factory = new BoltFactory();
        factory.registerProducer(new EventProcessingBoltProducer(converter));
        Map<String, Integer> aggregation = properties.get("aggregation");
        AggregationLimits limits = new AggregationLimits(aggregation.get("mins"), aggregation.get("count"));
        factory.registerProducer(new AggregationBoltProducer(limits));
        factory.registerProducer(new FileSystemBoltProducer(Paths.get(properties.<String>get("destination")),
                                                            Paths.get(properties.<String>get("schemaFile")).toFile()));
        factory.registerProducer(new TransferBoltProducer(messageProducer));
        return factory;
    }

    private SpoutConfig getSpoutConfig(BrokerHosts zk, String topic, String id) {
        SpoutConfig spoutConf = new SpoutConfig(zk, properties.get(topic), "", id);
        spoutConf.forceFromStart = true;
        spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
        return spoutConf;
    }
}
