package com.boraydata.transfer;

import com.boraydata.env.Attribute;

public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT>{

    /**
     * Processes one element that arrived at this operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    void processElement(Attribute<IN> element) throws Exception;


}
