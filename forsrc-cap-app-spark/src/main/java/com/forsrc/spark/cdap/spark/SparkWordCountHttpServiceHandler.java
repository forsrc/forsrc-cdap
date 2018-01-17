package com.forsrc.spark.cdap.spark;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import com.google.common.base.Charsets;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class SparkWordCountHttpServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("sparkWordCountKeyValueTable")
    private KeyValueTable kvKeyValueTable;

    private Metrics metrics;

    @Path("sparkWordCount")
    @GET
    public void greet(HttpServiceRequest request, HttpServiceResponder responder) {
        byte[] name = kvKeyValueTable.read(SparkWordCountFlowlet.NAME);
        String toGreet = name != null ? new String(name, Charsets.UTF_8) : "World";
        System.err.println("--> greet: " + toGreet);
        if (toGreet.equals("Jane Doe")) {
            metrics.count("greetings.count.jane_doe", 1);
        }
        responder.sendString(String.format("Hello %s!", toGreet));
    }
}