package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;


public class WordCountApplication extends AbstractApplication<WordCountConfig> {

    @SuppressWarnings("unchecked")
    @Override
    public void configure() {
        WordCountConfig config = getConfig();
        setName("WordCountApplication");
        setDescription("Example word count application");
        addStream(config.getStream());

        createDataset(config.getWordCountTableName(), Table.class,
                TableProperties.builder().setReadlessIncrementSupport(true)
                        .setDescription("Stats of total counts and lengths of words").build());
        
        createDataset(config.getAssociatorDatasetTableName(), WordCountAssociatorDataset.class,
                DatasetProperties.builder().setDescription("Word associations table").build());

        createDataset(config.getCounterTableName(), KeyValueTable.class,
                DatasetProperties.builder().setDescription("Words and corresponding counts").build());

        createDataset(config.getUniqueDatasetTableName(), WordCountUniqueDataset.class,
                DatasetProperties.builder().setDescription("Total count of unique words").build());

        addFlow(new WordCountFlow(config));

        addService(new WordCountService(config));
    }

}
