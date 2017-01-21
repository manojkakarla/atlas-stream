package com.atlas.stream.stream.topology;


import com.atlas.stream.stream.bolt.AggregationBolt;
import com.atlas.stream.stream.bolt.FileSystemBolt;
import com.atlas.stream.stream.bolt.TransferBolt;
import com.atlas.stream.stream.bolt.factory.BoltFactory;
import com.atlas.stream.stream.bolt.EventProcessingBolt;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

public class TopologyFactory {

    public static final String KAFKA_SPOUT = "kafkaSpout";
    public static final String EVENT_PROCESSING_BOLT = "eventProcessingBolt";
    public static final String AGGREGATION_BOLT = "aggregationBolt";
    public static final String FILE_SYSTEM_BOLT = "fileSystemBolt";
    public static final String TRANSFER_BOLT = "transferBolt";
    public static final String CMD_SPOUT = "cmdSpout";

    private final SpoutConfig spoutConf;
    private final SpoutConfig cmdConf;
    private final BoltFactory boltFactory;

    public TopologyFactory(SpoutConfig spoutConf, SpoutConfig cmdConf, BoltFactory boltFactory) {

        this.spoutConf = spoutConf;
        this.cmdConf = cmdConf;
        this.boltFactory = boltFactory;
    }

    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
        KafkaSpout cmdSpout = new KafkaSpout(cmdConf);

        builder.setSpout(KAFKA_SPOUT, kafkaSpout);
        builder.setBolt(EVENT_PROCESSING_BOLT, getBolt(EventProcessingBolt.class)).shuffleGrouping(KAFKA_SPOUT);
        AggregationBolt aggregationBolt = getBolt(AggregationBolt.class);
        builder.setBolt(AGGREGATION_BOLT, aggregationBolt).shuffleGrouping(EVENT_PROCESSING_BOLT);
        builder.setBolt(FILE_SYSTEM_BOLT, getBolt(FileSystemBolt.class)).shuffleGrouping(AGGREGATION_BOLT);
        builder.setBolt(TRANSFER_BOLT, getBolt(TransferBolt.class)).shuffleGrouping(FILE_SYSTEM_BOLT);

        builder.setSpout(CMD_SPOUT, cmdSpout);
        builder.setBolt("cmdAggBolt", aggregationBolt).allGrouping(CMD_SPOUT);

        return builder.createTopology();
    }

    private <T extends IRichBolt> T getBolt(Class<T> boltClass) {
        return boltFactory.boltProducer(boltClass).create();
    }
}
