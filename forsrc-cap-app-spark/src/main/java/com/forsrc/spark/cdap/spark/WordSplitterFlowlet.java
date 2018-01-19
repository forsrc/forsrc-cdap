package com.forsrc.spark.cdap.spark;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

public class WordSplitterFlowlet extends AbstractFlowlet {

    @UseDataSet("sparkWordCountDataset")
    private KeyValueTable kvKeyValueTable;

    @Output("wordOut")
    private OutputEmitter<String> wordOutput;


    @Override
    public void initialize(FlowletContext context) throws Exception {
        super.initialize(context);
    }

    @ProcessInput
    public void process(StreamEvent event) {

        // Input is a String, need to split it by whitespace
        String inputString = Charset.forName("UTF-8").decode(event.getBody()).toString();

        String[] words = inputString.split("\\s+");

        long wordCount = 0;

        // We have an array of words, now remove all non-alpha characters
        for (String word : words) {
            word = word.replaceAll("[^A-Za-z]", "");
            if (!word.isEmpty()) {
                // emit every word that remains
                wordOutput.emit(word);
                wordCount++;
            }
        }

        // Count other word statistics (word length, total words seen)
        this.kvKeyValueTable.incrementAndGet(Bytes.toBytes("total_words"), wordCount);


    }
}
