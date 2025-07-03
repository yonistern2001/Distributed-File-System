package org.example.filesystem.exception;

import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@NoArgsConstructor
@ResponseStatus(HttpStatus.INSUFFICIENT_STORAGE)
public class InsufficientStorageException extends RuntimeException {

    public InsufficientStorageException(String msg) {
        super(msg);
    }
}
