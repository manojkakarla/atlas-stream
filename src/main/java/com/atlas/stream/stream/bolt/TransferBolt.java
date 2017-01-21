package com.atlas.stream.stream.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.atlas.core.MessageProducer;
import com.atlas.stream.stream.domain.TransferMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class TransferBolt implements IRichBolt {

    private final MessageProducer<TransferMessage> messageProducer;
    private OutputCollector _collector;

    public TransferBolt(MessageProducer<TransferMessage> messageProducer) {
        this.messageProducer = messageProducer;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String filename = tuple.getStringByField("filename");
            messageProducer.send(new TransferMessage(filename), "TRANSFER");

            _collector.ack(tuple);
        } catch (Exception e) {
            log.error("Failed to process transfer message", e);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
