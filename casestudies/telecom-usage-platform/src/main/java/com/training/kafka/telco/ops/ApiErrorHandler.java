package com.training.kafka.telco.ops;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Turns Kafka errors into readable JSON. The "title" is the Kafka exception
 * class and the "detail" is the broker's own message.
 */
@RestControllerAdvice
public class ApiErrorHandler {

    @ExceptionHandler(ExecutionException.class)
    ProblemDetail kafkaError(ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        HttpStatus status = HttpStatus.BAD_REQUEST;
        if (cause instanceof UnknownTopicOrPartitionException) {
            status = HttpStatus.NOT_FOUND;
        } else if (cause instanceof TopicExistsException) {
            status = HttpStatus.CONFLICT;
        } else if (!(cause instanceof InvalidConfigurationException)
                && !(cause instanceof org.apache.kafka.common.errors.InvalidRequestException)) {
            status = HttpStatus.BAD_GATEWAY;
        }
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, cause.getMessage());
        problem.setTitle(cause.getClass().getSimpleName());
        return problem;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badRequest(IllegalArgumentException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("BadRequest");
        return problem;
    }

    @ExceptionHandler(TimeoutException.class)
    ProblemDetail timeout(TimeoutException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.GATEWAY_TIMEOUT,
                "No answer from the Kafka cluster in time. Are the brokers (and a controller majority) up?");
        problem.setTitle("TimeoutException");
        return problem;
    }
}
