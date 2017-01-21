package com.atlas.stream.stream.bolt.factory;

import com.atlas.core.MessageProducer;
import com.atlas.stream.stream.bolt.TransferBolt;
import com.atlas.stream.stream.domain.TransferMessage;

public class TransferBoltProducer extends BoltProducer<TransferBolt> {

    private final MessageProducer<TransferMessage> messageProducer;

    public TransferBoltProducer(MessageProducer<TransferMessage> messageProducer) {
        super(TransferBolt.class);
        this.messageProducer = messageProducer;
    }

    @Override
    public TransferBolt create() {
         return new TransferBolt(messageProducer);
    }
}
