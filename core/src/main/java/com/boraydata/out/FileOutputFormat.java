package com.boraydata.out;

import com.boraydata.fs.WriteMode;

public abstract class FileOutputFormat<IT>  implements OutputFormat<IT>   {





    protected final String path;

    protected final WriteMode writeMode ;

    public FileOutputFormat(String path, WriteMode writeMode) {
        this.path = path;
        this.writeMode = writeMode;
    }
}
