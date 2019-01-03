package test;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wufj(feijian_wu@kingdee.com)
 * @since 2018/7/25 23:12
 */
public class DoubleQueueTests {

  private static AtomicBoolean a = new AtomicBoolean(false);
  public static void main(String[] args) {
    final DoubleQueueTests doubleQueueTests = new DoubleQueueTests();
    doubleQueueTests.start();
    for (int i = 0; i < 100000; i++) {
      new Thread(() -> doubleQueueTests.send("msg")).start();
    }
  }

  private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue();

  public void send(String msg) {
    try {
      queue.put(msg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void start() {
    new Thread(new consumer()).start();
  }

  class consumer implements Runnable {
    private AtomicInteger counter = new AtomicInteger();
    @Override
    public void run() {
      ArrayList buffer = new ArrayList(100);
      while (true) {
        try {
          final String poll = queue.take();
          buffer.add(poll);
          if (buffer.size() >= 100) {
            System.out.println(counter.incrementAndGet() + "-" + buffer.size());
            buffer = new ArrayList();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

}
