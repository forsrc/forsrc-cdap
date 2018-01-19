package com.forsrc.spark.cdap.spark;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class SparkWordCountHttpServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet(SparkWordCountConfig.KEY_VALUE_DATASET)
    private KeyValueTable kvKeyValueTable;

    private Metrics metrics;

    private static final Logger LOG = LoggerFactory.getLogger(SparkWordCountHttpServiceHandler.class);

    @Path("sparkWordCount/{word}")
    @GET
    public void greet(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("word") String word) {

        byte[] countBytes = this.kvKeyValueTable.read(word.trim());
        LOG.info("word: {} -> countBytes: {}", word, countBytes);
        long count = countBytes != null ? Bytes.toLong(countBytes) : 0L;
        System.err.println(String.format("--> key: %s, count: %s", word, count));

        metrics.count(word, (int)count);
        Map<String, Object> results = new HashMap<>();
        results.put("word", word);
        results.put("count", count);
        LOG.info("results: {}", results);
        responder.sendJson(results);
    }
}
