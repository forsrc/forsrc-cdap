package com.forsrc.spark.cdap.spark;

import co.cask.cdap.api.Config;

public class SparkWordCountConfig extends Config {
    private String stream = "sparkWordCountStream";
    private String saver = "sparkWordCountSaver";
    private String flowName = "sparkWordCountFlow";

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

}
