package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;

public class SparkWordCountFlowlet extends AbstractFlowlet {



    @UseDataSet(SparkWordCountConfig.KEY_VALUE_DATASET)
    private KeyValueTable kvKeyValueTable;

    private Metrics metrics;

//    @ProcessInput
//    public void process(StreamEvent event) {
//        String word = Bytes.toString(event.getBody());
//     // Count number of times we have seen this word
//        this.kvKeyValueTable.increment(Bytes.toBytes(word), 1L);
//        metrics.count("word."  + word, Bytes.toInt(this.kvKeyValueTable.read(word)));
//    }

    @ProcessInput("wordOut")
    public void process(String word) {
        // Count number of times we have seen this word
        this.kvKeyValueTable.increment(Bytes.toBytes(word), 1L);
        metrics.count("word."  + word, Bytes.toInt(this.kvKeyValueTable.read(word)));

    }
}
