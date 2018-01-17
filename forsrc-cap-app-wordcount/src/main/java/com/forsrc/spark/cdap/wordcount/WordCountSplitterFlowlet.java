package com.forsrc.spark.cdap.wordcount;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

public class WordCountSplitterFlowlet extends AbstractFlowlet {

    private Table table;

    @Output("wordOut")
    private OutputEmitter<String> wordOutput;

    @Output("wordArrayOut")
    private OutputEmitter<List<String>> wordListOutput;

    @Property
    private final String tableName;

    public WordCountSplitterFlowlet(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
        super.initialize(context);
        this.table = context.getDataset(tableName);
    }

    @ProcessInput
    public void process(StreamEvent event) {

        // Input is a String, need to split it by whitespace
        String inputString = Charset.forName("UTF-8").decode(event.getBody()).toString();

        String[] words = inputString.split("\\s+");
        List<String> wordList = new ArrayList<>(words.length);

        long sumOfLengths = 0;
        long wordCount = 0;

        // We have an array of words, now remove all non-alpha characters
        for (String word : words) {
            word = word.replaceAll("[^A-Za-z]", "");
            if (!word.isEmpty()) {
                // emit every word that remains
                wordOutput.emit(word);
                wordList.add(word);
                sumOfLengths += word.length();
                wordCount++;
            }
        }

        // Count other word statistics (word length, total words seen)
        this.table.increment(new Increment("totals").add("total_length", sumOfLengths).add("total_words", wordCount));

        // Send the list of words to the associater
        wordListOutput.emit(wordList);

    }
}
