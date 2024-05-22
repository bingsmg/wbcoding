package live.clover.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author weibb
 */
public class ThreadPollConsumerGroupDemo {

    public void groupConsumer() {
        ExecutorService executorService = Executors.newFixedThreadPool(6);
    }
}
