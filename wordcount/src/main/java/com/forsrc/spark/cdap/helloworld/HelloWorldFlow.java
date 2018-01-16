package com.forsrc.spark.cdap.helloworld;

import co.cask.cdap.api.flow.AbstractFlow;


public class HelloWorldFlow extends AbstractFlow {

    @Override
    protected void configure() {
        HelloWorldConfig config = new HelloWorldConfig();
        setName(config.getFlowName());
        setDescription("A flow that collects names");
        addFlowlet(config.getSaver(), new HelloWorldFlowlet());
        connectStream(config.getStream(), config.getSaver());
    }
}