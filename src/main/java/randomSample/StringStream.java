package randomSample;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class StringStream implements SourceFunction<String> {

    private long seed;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
            Thread.sleep(5000);
            ctx.collect(RandomStringUtils.randomAlphabetic(100));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
