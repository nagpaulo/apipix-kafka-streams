package com.alura.pix.exception;

public class KeyNotFoundException extends RuntimeException {
    public KeyNotFoundException() {
    }

    public KeyNotFoundException(String message) {
        super(message);
    }

    public KeyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public KeyNotFoundException(Throwable cause) {
        super(cause);
    }

    public KeyNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
