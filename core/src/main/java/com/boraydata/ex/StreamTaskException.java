package com.boraydata.ex;

import java.io.IOException;

public class StreamTaskException extends RuntimeException {

    /**
     * Serial version UID for serialization interoperability.
     */
    private static final long serialVersionUID = 8392043527067472439L;

    /**
     * Creates a compiler exception with no message and no cause.
     */
    public StreamTaskException() {
    }

    /**
     * Creates a compiler exception with the given message and no cause.
     *
     * @param message
     *            The message for the exception.
     */
    public StreamTaskException(String message) {
        super(message);
    }

    /**
     * Creates a compiler exception with the given cause and no message.
     *
     * @param cause
     *            The <tt>Throwable</tt> that caused this exception.
     */
    public StreamTaskException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a compiler exception with the given message and cause.
     *
     * @param message
     *            The message for the exception.
     * @param cause
     *            The <tt>Throwable</tt> that caused this exception.
     */
    public StreamTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
