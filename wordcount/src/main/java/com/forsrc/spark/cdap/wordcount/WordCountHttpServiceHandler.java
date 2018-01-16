package com.forsrc.spark.cdap.wordcount;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class WordCountHttpServiceHandler extends AbstractHttpServiceHandler {

    @Property
    private final String keyValueTableName;

    private KeyValueTable keyValueTable;

    public WordCountHttpServiceHandler(WordCountConfig config) {
        this.keyValueTableName = config.getCounterTableName();
    }

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
        super.initialize(context);
        this.keyValueTable = context.getDataset(keyValueTableName);
    }

    /**
     * Returns the count for a specific word and its word associations, up to the
     * specified limit or a pre-set limit of ten if not specified.
     */
    @Path("count/{word}")
    @GET
    public void getCount(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("word") String word,
            @DefaultValue("10") @QueryParam("limit") Integer limit) {
        // Read the word count
        byte[] countBytes = keyValueTable.read(Bytes.toBytes(word));
        long wordCount = countBytes == null ? 0L : Bytes.toLong(countBytes);

        // Build a map with results
        Map<String, Object> results = new HashMap<>();
        results.put("word", word);
        results.put("count", wordCount);

        responder.sendJson(results);
    }

}
