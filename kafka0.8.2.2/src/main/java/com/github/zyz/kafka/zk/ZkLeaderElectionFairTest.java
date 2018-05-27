package com.github.zyz.kafka.zk;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zyz
 * @since 2018/5/25
 */
public class ZkLeaderElectionFairTest {

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch mainLatch = new CountDownLatch(1);

        ThreadFactory threadFactory = new ThreadFactory() {

            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "client-" + threadNumber.getAndIncrement());
                if (t.isDaemon()) t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
                return t;
            }
        };

        ExecutorService executorService = new ThreadPoolExecutor(3,3,0,
                TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(10),threadFactory);

        ElectionCallback callback = new ElectionCallback() {

            @Override
            public void becomeLeader(String leaderPath) {
                System.err.format(Thread.currentThread().getName() + " callback : node %s become ther leader\n", leaderPath);
            }
        };

        for (int i =0;i< 3;i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    ZkLeaderElectionFair leaderElection = ZkLeaderElectionFairFactory.builder().connectString("202.204.71.10:2181").sessionTimeout(8000)
                            .rootPath("/leaderRoot").callback(callback).build();
                    boolean result = leaderElection.start(false);
                    System.out.println(Thread.currentThread().getName() + "elctor leader result : " + result);
                }
            });
        }

        executorService.shutdown();
        mainLatch.await();
    }
}
