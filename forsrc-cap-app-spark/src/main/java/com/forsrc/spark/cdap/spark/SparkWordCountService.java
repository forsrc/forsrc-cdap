package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.service.AbstractService;


public class SparkWordCountService extends AbstractService {

    public static final String SERVICE_NAME = "sparkWordCountService";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      setDescription("Service that creates a greeting using a user's name.");
      addHandler(new SparkWordCountHttpServiceHandler());
    }
  }
