package com.atlas.stream.stream.bolt.factory;

import com.atlas.stream.stream.bolt.FileSystemBolt;

import java.io.File;
import java.nio.file.Path;

public class FileSystemBoltProducer extends BoltProducer<FileSystemBolt> {


    private final Path destination;
    private final File schema;

    public FileSystemBoltProducer(Path destination, File schema) {
        super(FileSystemBolt.class);
        this.destination = destination;
        this.schema = schema;
    }

    @Override
    public FileSystemBolt create() {
         return new FileSystemBolt(destination, schema);
    }
}
