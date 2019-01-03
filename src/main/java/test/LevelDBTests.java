package test;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

/**
 * @author wufj(feijian_wu@kingdee.com)
 * @since 2019/1/3 22:34
 */
@SuppressWarnings("all")
public class LevelDBTests {

  public static void main(String[] args) throws Exception {
    testRead();//62,78
    //testWrite();//299,282
  }

  private static void testRead() throws Exception {
    final DB db = getDB();

    AtomicInteger counter = new AtomicInteger();
    final long start = System.currentTimeMillis();
    final DBIterator iterator = db.iterator();
    while (iterator.hasNext()) {
      final Entry<byte[], byte[]> next = iterator.next();
      counter.getAndIncrement();
    }
    System.out.println(counter.get());
    System.out.println(System.currentTimeMillis() - start);
  }

  private static void testWrite() throws Exception {
    int timeSecond = (int) (System.currentTimeMillis() / 1000);

    final DB db = getDB();

    final long start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      final Key key = new Key();
      key.setLink(1);
      key.setTimestamp(timeSecond - 10);

      final Value value = new Value();
      value.setValue(1);
      value.setScore(2);
      value.setAverage(2);

      db.put(key.bytes(), value.bytes());
    }
    System.out.println(System.currentTimeMillis() - start);
  }

  @SuppressWarnings("all")
  private static DB getDB() throws Exception {
    Options options = new Options();
    options.createIfMissing(true);
    DB db = factory.open(new File("D:/", "leveldb"), options);
    return db;
  }

  static class Key {
    private int link;
    private int timestamp;

    public byte[] bytes() {
      return ByteBuffer
          .allocate(8)
          .putInt(link)
          .putInt(timestamp)
          .array();
    }

    public int getLink() {
      return link;
    }

    public Key setLink(int link) {
      this.link = link;
      return this;
    }

    public int getTimestamp() {
      return timestamp;
    }

    public Key setTimestamp(int timestamp) {
      this.timestamp = timestamp;
      return this;
    }
  }

  static class Value {
    private double value;
    private double score;
    private double average;

    public byte[] bytes() {
      return ByteBuffer
          .allocate(24)
          .putDouble(value)
          .putDouble(score)
          .putDouble(average)
          .array();
    }

    public double getValue() {
      return value;
    }

    public Value setValue(double value) {
      this.value = value;
      return this;
    }

    public double getScore() {
      return score;
    }

    public Value setScore(double score) {
      this.score = score;
      return this;
    }

    public double getAverage() {
      return average;
    }

    public Value setAverage(double average) {
      this.average = average;
      return this;
    }
  }
}
