package com.cdcex.server;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestControllerAdvice
public final class ApiErrors {
    @ExceptionHandler(Exception.class) public ResponseEntity<Map<String,String>> error(Exception e){
        HttpStatus status=e instanceof NoSuchElementException?HttpStatus.NOT_FOUND:e instanceof IllegalArgumentException||e instanceof org.springframework.http.converter.HttpMessageNotReadableException?HttpStatus.BAD_REQUEST:e instanceof IllegalStateException||e instanceof java.sql.SQLException?HttpStatus.CONFLICT:HttpStatus.BAD_GATEWAY;
        String message=switch(status){case NOT_FOUND->"Resource not found";case BAD_REQUEST->"Invalid configuration. Check required fields, mapping rules and connector prerequisites.";case CONFLICT->"Operation blocked. Check task state, resource ownership, destination schema and recovery requirements.";default->"Connection operation failed. Check reachability, credentials, TLS and required permissions.";};
        return ResponseEntity.status(status).body(Map.of("code",status.name(),"message",message));
    }
}
