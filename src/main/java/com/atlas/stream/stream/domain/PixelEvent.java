package com.atlas.stream.stream.domain;

import java.io.Serializable;

import lombok.Data;

@Data
public class PixelEvent implements Serializable {

    private String id;
    private String adId;
    private String deviceId;
    private long timeStamp;

}
