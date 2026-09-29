package com.training.kafka.admin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Turns Kafka errors into readable HTTP responses. The "title" is the Kafka
 * exception class and the "detail" is the broker's message: the same text the
 * CLI prints after "Error while executing topic command :".
 */
@RestControllerAdvice
public class ApiErrorHandler {

    @ExceptionHandler(ExecutionException.class)
    ProblemDetail kafkaError(ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        HttpStatus status = HttpStatus.BAD_REQUEST;
        if (cause instanceof UnknownTopicOrPartitionException || cause instanceof GroupIdNotFoundException) {
            status = HttpStatus.NOT_FOUND;
        } else if (cause instanceof TopicExistsException) {
            status = HttpStatus.CONFLICT;
        }
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, cause.getMessage());
        problem.setTitle(cause.getClass().getSimpleName());
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
