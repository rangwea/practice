package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author wufj(feijian_wu@kingdee.com)
 * @since 2018/5/30 22:32
 */
public class Test {
  private volatile Map<String, Object> map = new HashMap<>();
  private volatile boolean flushing = false;

  public static void main(String[] args) throws Exception {
    List<Test> tests = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final Test test = new Test();
      tests.add(test);
      new Thread(test::set).start();
    }

    Thread.sleep(10000);

    tests.parallelStream().forEach(Test::get);

    Thread.sleep(100000);
  }

  public void set() {
    while (flushing) {
      System.out.println("============");
    }
    map = new HashMap<>();
  }

  public void get() {
    try {
      System.out.println(map);
      flushing = true;
      final Long sleep = new Long(new Random().nextInt(2000));
      System.out.println(sleep);
      Thread.sleep(sleep);
      map = null;
      flushing = false;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
