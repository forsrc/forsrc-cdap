package com.forsrc.spark.cdap.helloworld;

import co.cask.cdap.api.flow.AbstractFlow;


public class HelloWorldFlow extends AbstractFlow {

    @Override
    protected void configure() {
        setName("WhoFlow");
        setDescription("A flow that collects names");
        addFlowlet("saver", new HelloWorldFlowlet());
        connectStream("who", "saver");
    }
}