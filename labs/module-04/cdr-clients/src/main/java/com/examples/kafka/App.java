package com.examples.kafka;

import java.util.Arrays;
import java.util.List;

import com.examples.kafka.review.CopilotDraftProducer;

/**
 * One entry point for the Module 4 labs:
 *
 *   java -jar target/cdr-clients.jar produce <topic> <count> [config=value ...]
 *   java -jar target/cdr-clients.jar consume <topic> <group> [options] [config=value ...]
 *   java -jar target/cdr-clients.jar rate    <in-topic> <out-topic> <group> [--abort-every=N]
 *   java -jar target/cdr-clients.jar draft   <topic>
 */
public final class App {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }
        List<String> rest = Arrays.asList(args).subList(1, args.length);
        switch (args[0]) {
            case "produce" -> CdrProducer.run(rest);
            case "consume" -> CdrConsumer.run(rest);
            case "rate" -> TransactionalRater.run(rest);
            case "draft" -> CopilotDraftProducer.run(rest.get(0));
            default -> usage();
        }
    }

    private static void usage() {
        System.err.println("""
                usage: java -jar target/cdr-clients.jar <command> ...
                  produce <topic> <count> [config=value ...]
                  consume <topic> <group> [--commit=after|before|auto] [--process-ms=N]
                          [--json] [--dlt=<topic>] [config=value ...]
                  rate    <in-topic> <out-topic> <group> [--abort-every=N]
                  draft   <topic>""");
        System.exit(1);
    }
}
