package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.flow.AbstractFlow;

public class WordCountFlow extends AbstractFlow {

    private WordCountConfig config;

    public WordCountFlow(WordCountConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        setName("WordCounterFlow");
        setDescription("Example Word Count Flow");

        addFlowlet("splitter", new WordCountSplitterFlowlet(config.getWordCountTableName()));
        addFlowlet("associator", new WordCountAssociatorFlowlet(config.getAssociatorDatasetTableName()));
        addFlowlet("counter", new WordCountCounterFlowlet(config.getCounterTableName()));
        addFlowlet("unique", new WordCountUniqueFlowlet(config.getUniqueDatasetTableName()));

        connectStream(config.getStream(), "splitter");
        connect("splitter", "associator");
        connect("splitter", "counter");
        connect("counter", "unique");
    }
}
