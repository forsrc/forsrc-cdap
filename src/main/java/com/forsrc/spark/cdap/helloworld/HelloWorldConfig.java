package com.forsrc.spark.cdap.helloworld;

import co.cask.cdap.api.Config;

public class HelloWorldConfig extends Config {
    private String stream = "helloworldStream";
    private String saver = "hellowordldSaver";
    private String flowName = "helloworldFlow";

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
