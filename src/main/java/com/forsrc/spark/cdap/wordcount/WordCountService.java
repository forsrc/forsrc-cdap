package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.service.AbstractService;

public class WordCountService extends AbstractService {

    public static final String SERVICE_NAME = "WordCountService";

    private WordCountConfig config;

    public WordCountService(WordCountConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        setName(SERVICE_NAME);
        setDescription("A service to retrieve statistics, word counts, and associations.");
        addHandler(new WordCountHttpServiceHandler(config));
    }

}
