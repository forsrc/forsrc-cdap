package com.forsrc.spark.cdap.helloworld;

import co.cask.cdap.api.service.AbstractService;


public class HelloWorldService extends AbstractService {

    public static final String SERVICE_NAME = "Greeting";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      setDescription("Service that creates a greeting using a user's name.");
      addHandler(new HelloWorldHttpServiceHandler());
    }
  }
