package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;

public class WordCountCounterFlowlet extends AbstractFlowlet {

    @Property
    private final String tableName;

    private KeyValueTable keyValueTable;

    private OutputEmitter<String> outputEmitter;

    private Metrics metrics;

    private int longestWordLength = 0;

    public WordCountCounterFlowlet(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
        super.initialize(context);
        this.keyValueTable = context.getDataset(tableName);
    }

    @ProcessInput("wordOut")
    public void process(String word) {
        // Count number of times we have seen this word
        this.keyValueTable.increment(Bytes.toBytes(word), 1L);
        if (word.length() > this.longestWordLength) {
            this.longestWordLength = word.length();
            this.metrics.gauge("longest.word.length", this.longestWordLength);
        }
        // Forward the word to the unique counter Flowlet to do the unique count
        outputEmitter.emit(word);
    }
}
