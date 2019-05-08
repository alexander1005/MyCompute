package com.boraydata.io;

import java.io.Serializable;

public interface InputSplit extends Serializable {

    /**
     * Returns the number of this input split.
     *
     * @return the number of this input split
     */
    int getSplitNumber();
}
