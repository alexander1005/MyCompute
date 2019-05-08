package com.boraydata.collect;

public interface Collector<Attribute> {

    /**
     * Emits a record.
     *
     * @param record The record to collect.
     */
   public void collect(Attribute record);

    /**
     * Closes the collector. If any data was buffered, that data will be flushed.
     */
   public void close();


   public Attribute getNext();
}
