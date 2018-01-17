package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;

public class SparkWordCountFlowlet extends AbstractFlowlet {

    static final byte[] NAME = Bytes.toBytes("name");

    @UseDataSet("sparkWordCountKeyValueTable")
    private KeyValueTable kvKeyValueTable;

    private Metrics metrics;

    @ProcessInput
    public void process(StreamEvent event) {
        byte[] name = Bytes.toBytes(event.getBody());
        if (name.length > 0) {
            kvKeyValueTable.write(NAME, name);

            if (name.length > 10) {
                metrics.count("names.longnames", 1);
            }
            metrics.count("names.bytes", name.length);
        }
    }
}
