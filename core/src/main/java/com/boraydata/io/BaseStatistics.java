package com.boraydata.io;

public interface BaseStatistics {

    /**
     * Constant indicating that the input size is unknown.
     */
    
    public static final long SIZE_UNKNOWN = -1;

    /**
     * Constant indicating that the number of records is unknown;
     */
    
    public static final long NUM_RECORDS_UNKNOWN = -1;

    /**
     * Constant indicating that average record width is unknown.
     */
    
    public static final float AVG_RECORD_BYTES_UNKNOWN = -1.0f;

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the total size of the input.
     *
     * @return The total size of the input, in bytes.
     */
    
    public long getTotalInputSize();

    /**
     * Gets the number of records in the input (= base cardinality).
     *
     * @return The number of records in the input.
     */
    
    public long getNumberOfRecords();

    /**
     * Gets the average width of a record, in bytes.
     *
     * @return The average width of a record in bytes.
     */
    
    public float getAverageRecordWidth();
}

