package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.Config;

public class WordCountConfig extends Config {

    private String stream = "WordCount-Stream";
    private String wordCountTableName = "WordCount-WordCountTable";
    private String uniqueDatasetTableName = "WordCountWordCount-UniqueDatasetTable";
    private String counterTableName = "WordCount-CounterTable";
    private String associatorDatasetTableName = "WordCount-AssociatorDatasetTable";

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public String getUniqueDatasetTableName() {
        return uniqueDatasetTableName;
    }

    public void setUniqueDatasetTableName(String uniqueDatasetTableName) {
        this.uniqueDatasetTableName = uniqueDatasetTableName;
    }

    public String getCounterTableName() {
        return counterTableName;
    }

    public void setCounterTableName(String counterTableName) {
        this.counterTableName = counterTableName;
    }

    public String getWordCountTableName() {
        return wordCountTableName;
    }

    public void setWordCountTableName(String wordCountTableName) {
        this.wordCountTableName = wordCountTableName;
    }

    public String getAssociatorDatasetTableName() {
        return associatorDatasetTableName;
    }

    public void setAssociatorDatasetTableName(String associatorDatasetTableName) {
        this.associatorDatasetTableName = associatorDatasetTableName;
    }

}
