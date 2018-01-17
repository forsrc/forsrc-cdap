package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

public class SparkWordCountApplication extends AbstractApplication<SparkWordCountConfig> {

    @Override
    public void configure() {
        setName("HelloWorld");
        setDescription("A Hello World program for the Cask Data Application Platform");
        addStream(new Stream(getConfig().getStream()));
        createDataset("whom", KeyValueTable.class, DatasetProperties.builder().setDescription("Store names").build());
        addFlow(new SparkWordCountFlow());
        addService(new SparkWordCountService());

    }

}
