package io.github.kn.flow.aws.sfn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.kn.flow.CompletionStageItemProcessor;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 *
 */
abstract class AbstractSfnProcessor<T, R> implements CompletionStageItemProcessor<T,
        R> {
    private final SfnAsyncClient sfnAsyncClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public AbstractSfnProcessor(final SfnAsyncClient client) {
        this.sfnAsyncClient = client;
    }

    protected SfnAsyncClient getSfnAsyncClient() {
        return sfnAsyncClient;
    }

    protected <V> V getObject(final String string, final Class<V> clazz) {
        try {
            return objectMapper.readValue(string, clazz);
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception parsing JSON string :" + string, e);
        }
    }

    private <V> Optional<String> getIfJSON(final V obj) {
        if (obj instanceof CharSequence) {
            return Optional.of(String.valueOf(obj).trim())
                    .filter(this::isValidJSON);
        }
        return Optional.empty();
    }

    private boolean isValidJSON(final String str) {
        try {
            objectMapper.readTree(str);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private String getJsonized(final SimpleJsonInputOutput io) {
        try {
            return objectMapper.writeValueAsString(io);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Exception forming JSON", e);
        }
    }

    private <V> Optional<SimpleJsonInputOutput> getIfIO(final V obj) {
        Optional<SimpleJsonInputOutput> returnOpt = Optional.empty();
        if (obj instanceof SimpleJsonInputOutput) {
            returnOpt = Optional.of((SimpleJsonInputOutput) obj);
        }
        return returnOpt;
    }

    private <V> String getJsonized(final V val, final Supplier<SimpleJsonInputOutput> io) {
        return getIfJSON(val)
                .orElse(getJsonized(getIfIO(val).orElseGet(io)));
    }

    protected <V> String getJsonizedOutput(final V outp) {
        return getJsonized(outp, () -> new SimpleJsonInputOutput().setOutput(outp));
    }

    protected <V> String getJsonizedInput(final V inp) {
        return getJsonized(inp, () -> new SimpleJsonInputOutput().setInput(inp));
    }

    protected static class SimpleJsonInputOutput {
        private Object output;
        private Object input;

        public Object getOutput() {
            return output;
        }

        public SimpleJsonInputOutput setOutput(final Object outp) {
            this.output = outp;
            return this;
        }

        public Object getInput() {
            return input;
        }

        public SimpleJsonInputOutput setInput(Object inp) {
            this.input = inp;
            return this;
        }
    }
}
