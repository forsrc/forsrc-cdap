package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.Config;

public class SparkWordCountConfig extends Config {

    public static final String KEY_VALUE_DATASET = "sparkWordCountDataset";

    private String stream = "sparkWordCountStream";
    private String saver = "sparkWordCountSaver";
    private String flowName = "sparkWordCountFlow";
    private String wordCountTableName = "sparkWordCount-WordCountTable";

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public String getSaver() {
        return saver;
    }

    public void setSaver(String saver) {
        this.saver = saver;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public String getWordCountTableName() {
        return wordCountTableName;
    }

    public void setWordCountTableName(String wordCountTableName) {
        this.wordCountTableName = wordCountTableName;
    }

}
