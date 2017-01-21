package com.atlas.stream.stream.bolt;

import com.atlas.client.JsonConverter;
import com.atlas.stream.stream.domain.PixelEvent;

import java.io.ByteArrayInputStream;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessingBolt implements IRichBolt {

    private OutputCollector _collector;
    private final JsonConverter converter;


    public EventProcessingBolt(JsonConverter converter) {
        this.converter = converter;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    private PixelEvent decodeAvro(Tuple tuple) {
        byte[] bytes = (byte[]) tuple.getValueByField("bytes");
        return converter.parseFromJson(new ByteArrayInputStream(bytes), PixelEvent.class);

    }

    public void execute(Tuple tuple) {
        try {
            PixelEvent record = decodeAvro(tuple);
            if (record != null) {
                log.info("emitting to stream: {}", record);
                _collector.emit(new Values(record));
            }
            _collector.ack(tuple);
        } catch (Throwable e) {
            log.error("failed to process message", e);
        }
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
