package com.atlas.stream.stream.bolt;

import com.google.common.base.Throwables;

import com.atlas.stream.stream.domain.PixelEvent;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
public class FileSystemBolt implements IRichBolt {

    private OutputCollector _collector;
    private final Path destination;
    private final Schema schema;

    public FileSystemBolt(Path destination, File schemaFile) {
        this(destination, createSchema(schemaFile));
    }

    private static Schema createSchema(File schemaFile) {
        try {
            return new Schema.Parser().parse(schemaFile);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private FileSystemBolt(Path destination, Schema schema) {
        this.destination = destination;
        this.schema = schema;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            List<PixelEvent> records = (List<PixelEvent>) tuple.getValueByField("events");
            String id = tuple.getStringByField("filename");
            Files.createDirectory(destination);
            DatumWriter<PixelEvent> writer = new ReflectDatumWriter<>(PixelEvent.class);
            DataFileWriter<PixelEvent> file = new DataFileWriter<>(writer);
            file.setMeta("version", 1);
            file.setCodec(CodecFactory.deflateCodec(5));
            File outputFile = destination.resolve(id).toFile();
            file.create(schema, outputFile);

            records.forEach(r -> {
                try {
                    file.append(r);
                } catch (Exception e) {
                    log.error("failed to write: {}", r);
                }
            });
            file.close();
            _collector.emit(new Values(outputFile.getAbsolutePath()));
            _collector.ack(tuple);
        } catch (Exception e) {
            log.error("Failed to process File message", e);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filename"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
