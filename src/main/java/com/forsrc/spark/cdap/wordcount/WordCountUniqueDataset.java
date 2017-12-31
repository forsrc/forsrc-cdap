package com.forsrc.spark.cdap.wordcount;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Table;

public class WordCountUniqueDataset extends AbstractDataset {

    private static final byte[] UNIQUE_COUNT = Bytes.toBytes("unique");
    private static final byte[] ENTRY_COUNT = Bytes.toBytes("count");

    private Table uniqueCountTable;
    private Table entryCountTable;

    public WordCountUniqueDataset(DatasetSpecification spec, @EmbeddedDataset("unique") Table uniqueCountTable,
            @EmbeddedDataset("entry") Table entryCountTable) {
        super(spec.getName(), uniqueCountTable, entryCountTable);
        this.uniqueCountTable = uniqueCountTable;
        this.entryCountTable = entryCountTable;

    }

    @ReadOnly
    public Long readUniqueCount() {
        return uniqueCountTable.get(new Get(UNIQUE_COUNT, UNIQUE_COUNT)).getLong(UNIQUE_COUNT, 0);
    }

    @WriteOnly
    public void updateUniqueCount(String entry) {
        long newCount = this.entryCountTable.incrementAndGet(Bytes.toBytes(entry), ENTRY_COUNT, 1L);
        if (newCount == 1L) {
            this.uniqueCountTable.increment(UNIQUE_COUNT, UNIQUE_COUNT, 1L);
        }
    }
}
