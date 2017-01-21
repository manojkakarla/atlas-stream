package com.atlas.stream.stream.bolt;

import com.atlas.stream.stream.domain.AggregationLimits;
import com.atlas.stream.stream.domain.PixelEvent;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
public class AggregationBolt implements IRichBolt {

    private List<PixelEvent> events = new LinkedList<>();
    private AggregationLimits aggregationLimits;
    private long lastThreshold = System.currentTimeMillis();
    private long count;

    private OutputCollector _collector;

    public AggregationBolt(AggregationLimits aggregationLimits) {
        this.aggregationLimits = aggregationLimits;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        try {
            if (tuple.contains("event")) {
                PixelEvent record = (PixelEvent) tuple.getValueByField("event");
                handleEvent(record);
            } else if (tuple.contains("persist")) {
                Boolean persist = tuple.getBooleanByField("persist");
                if (persist && count > 0) {
                    persist(events);
                }
            }
            _collector.ack(tuple);
        } catch (Throwable e) {
            log.error("failed to process agg message", e);
        }
    }

    private void handleEvent(PixelEvent record) {
        long timestamp = record.getTimeStamp();

        count++;
        if (thresholdReached(timestamp)) {
            log.info("Threshold reached at time: {}, count: {}", timestamp, count);
            List<PixelEvent> copy = events;
            //TODO emit data
            persist(copy);
            clear(timestamp);
        }
        events.add(record);

    }

    private void clear(long timestamp) {
        events = new ArrayList<>();
        lastThreshold = timestamp;
        count = 0;
    }

    private boolean thresholdReached(long timestamp) {
        return timestamp - lastThreshold > aggregationLimits.getPeriodMillis() ||
               count >= aggregationLimits.getCount();
    }

    private void persist(List<PixelEvent> data) {
        log.info("Persisting count: [{}]", data.size());
        _collector.emit(new Values(data));
    }

    public void cleanup() {
        persist(events);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("events"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
