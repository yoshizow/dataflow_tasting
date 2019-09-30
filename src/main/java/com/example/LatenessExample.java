package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatenessExample {
    private static final long WINDOW_DURATION_SECS = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(LatenessExample.class);

    static class GenerateEvent extends PTransform<PBegin, PCollection<String>> {
        static GenerateEvent fromEpoch() { return new GenerateEvent(0); }
        static GenerateEvent fromNow() { return new GenerateEvent(Instant.now().getMillis() / 1000); }

        private long startSeq;

        public GenerateEvent(long startSeq) {
            this.startSeq = startSeq;
        }

        public PCollection<String> expand(PBegin input) {
            return input
                .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1))
                    .withTimestampFn(seq -> {
                        int delaySecs = 0;
                        if (seq % WINDOW_DURATION_SECS == 0) {
                            delaySecs = 30;
                        }
                        return seqToTs(seq).minus(Duration.standardSeconds(delaySecs));
                    }))
                .apply(ParDo.of(new DoFn<Long, String>() {
                    // Convert sequence to timestamp string, with optional delay
                    @ProcessElement
                    public void processElement(@Element Long seq, @Timestamp Instant ts, OutputReceiver<String> receiver) {
                        Instant origTs = seqToTs(seq);
                        long diffSecs = (ts.getMillis() - origTs.getMillis()) / 1000;
                        String result = ts.toString();
                        if (diffSecs < 0) {
                            result = result + "(" + (-diffSecs) + "s late)";
                        } else if (diffSecs > 0) {
                            result = result + "(" + (diffSecs) + "s early)";
                        }
                        receiver.output(result);
                    }
                }));
        }

        private Instant seqToTs(long seq) {
            return Instant.ofEpochSecond(seq + this.startSeq);
        }
    }

    static PCollection<String> applyWindow(PCollection<String> events) {
        return events
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(WINDOW_DURATION_SECS)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withLateFirings(AfterProcessingTime
                                .pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))
                    )
                    .withAllowedLateness(Duration.standardSeconds(60))
                    .discardingFiredPanes());
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> events =
            p.apply(GenerateEvent.fromNow());

        PCollection<String> windowed = applyWindow(events);

        PCollection<String> output = windowed.apply(Combine.<String>globally(tsIterable -> {
            return String.join("\n", tsIterable) + "\n";
        }).withoutDefaults());

        output.apply(Log.ofElements());

        p.run();
    }
}
