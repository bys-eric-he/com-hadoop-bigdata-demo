package com.hadoop.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Locale;

/**
 * word count的processor实例
 */
@Slf4j
public class WordCountProcessorSupplier implements ProcessorSupplier<String, Processor> {
    @Override
    public Processor get() {
        return new WordCountProcessor();
    }

    private class WordCountProcessor implements Processor<String, String> {

        private ProcessorContext context;

        private KeyValueStore<String, Integer> kvStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.context.schedule(1000);
            this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
        }

        @Override
        public void process(String key, String value) {
            String[] words = value.toLowerCase(Locale.getDefault()).split(" ");
            for (String word : words) {
                Integer oldValue = this.kvStore.get(word);
                if (oldValue == null) {
                    this.kvStore.put(word, 1);
                } else {
                    this.kvStore.put(word, oldValue + 1);
                }
            }
        }

        /**
         * Perform any periodic operations
         *
         * @param timestamp
         */
        @Override
        public void punctuate(long timestamp) {
            try (KeyValueIterator<String, Integer> itr = this.kvStore.all()) {
                while (itr.hasNext()) {
                    KeyValue<String, Integer> entry = itr.next();
                    log.info("-->统计结果：[" + entry.key + ", " + entry.value + "]");
                    context.forward(entry.key, entry.value.toString());
                }
                context.commit();
            }
        }

        @Override
        public void close() {
            this.kvStore.close();
        }
    }
}