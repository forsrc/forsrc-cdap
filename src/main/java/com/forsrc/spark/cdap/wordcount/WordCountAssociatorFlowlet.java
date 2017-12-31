package com.forsrc.spark.cdap.wordcount;

import java.util.Set;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;

public class WordCountAssociatorFlowlet extends AbstractFlowlet {

    @Property
    private final String tableName;

    private WordCountAssociatorDataset table;

    public WordCountAssociatorFlowlet(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
        super.initialize(context);
        this.table = context.getDataset(tableName);
    }

    @ProcessInput
    public void process(Set<String> words) {

        // Store word associations
        this.table.writeWordAssocs(words);
    }
}
