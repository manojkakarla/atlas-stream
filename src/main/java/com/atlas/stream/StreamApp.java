package com.atlas.stream;

import com.atlas.core.AtlasAppBuilder;
import com.atlas.core.AtlasJmsAppBuilder;
import com.atlas.core.dw.AtlasApp;
import com.atlas.stream.config.StreamConfig;
import com.atlas.stream.dw.StreamAppConfig;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;

@Slf4j
public class StreamApp {

  public static void main(String[] args) throws Exception {
    AtlasApp<StreamAppConfig> app = createBuilder(StreamConfig.class).build();
    log.info("============== App built, starting now ==============");
    app.run(args);
  }

  protected static AtlasAppBuilder<StreamAppConfig> createBuilder(Class<?>... configs) {
    Function<StreamAppConfig, Map<String, Object>> beans = appConfig -> ImmutableMap.of(
            "database", appConfig.getDatabase());
    return new AtlasJmsAppBuilder<StreamAppConfig>()
            .withJms()
            .name("Atlas Stream - Storm")
            .withSpring(beans, configs)
            .withInfoResource();
  }

}
