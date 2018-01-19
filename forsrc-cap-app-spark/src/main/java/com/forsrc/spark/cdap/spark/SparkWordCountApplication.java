package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

public class SparkWordCountApplication extends AbstractApplication<SparkWordCountConfig> {

    @Override
    public void configure() {
        setName("sparkWordCountApplication");
        setDescription("A Hello World program for the Cask Data Application Platform");

        addStream(new Stream(getConfig().getStream()));

        createDataset(SparkWordCountConfig.KEY_VALUE_DATASET, KeyValueTable.class, DatasetProperties.builder().setDescription("Store names").build());

        addSpark(new SparkWordCountSpark());
        addWorkflow(new SparkWordCountWorkflow());

       //addFlow(new SparkWordCountFlow());

        addService(new SparkWordCountService());

    }

}
