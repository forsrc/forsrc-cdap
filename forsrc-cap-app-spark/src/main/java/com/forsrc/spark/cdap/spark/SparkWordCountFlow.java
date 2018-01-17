package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.flow.AbstractFlow;


public class SparkWordCountFlow extends AbstractFlow {

    @Override
    protected void configure() {
        SparkWordCountConfig config = new SparkWordCountConfig();
        setName(config.getFlowName());
        setDescription("SparkWordCountFlow");
        addFlowlet(config.getSaver(), new SparkWordCountFlowlet());
        connectStream(config.getStream(), config.getSaver());
    }
}