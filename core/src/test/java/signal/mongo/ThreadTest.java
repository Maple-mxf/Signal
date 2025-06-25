package signal.mongo;

import java.util.concurrent.TimeUnit;

public class ThreadTest {

    public static void main(String[] args) throws InterruptedException {


        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 10; i++) {
                    try {
                        TimeUnit.SECONDS.sleep(1L);
                        System.err.println(i);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });

        thread.start();

        TimeUnit.SECONDS.sleep(2L);

        thread.interrupt();

        TimeUnit.SECONDS.sleep(20L);

    }

}
