package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;

public class WordCountUniqueFlowlet extends AbstractFlowlet {

    @Property
    private final String tableName;

    private WordCountUniqueDataset table;

    public WordCountUniqueFlowlet(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
        super.initialize(context);
        this.table = context.getDataset(tableName);
    }

    @ProcessInput
    public void process(String word) {
        this.table.updateUniqueCount(word);
    }
}
