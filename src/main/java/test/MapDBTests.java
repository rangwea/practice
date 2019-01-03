package test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

/**
 * @author wufj(feijian_wu@kingdee.com)
 * @since 2019/1/3 22:34
 */
@SuppressWarnings("all")
public class MapDBTests {

  public static void main(String[] args) {
    //testRead();//66
    testWrite();
  }

  private static void testRead() {
    final Map<Key, Value> db = getDB();

    AtomicInteger counter = new AtomicInteger();
    final long start = System.currentTimeMillis();
    db.forEach((k, v) -> {
      counter.incrementAndGet();
    });
    System.out.println(counter.get());
    System.out.println(System.currentTimeMillis() - start);
  }

  private static void testWrite() {
    int timeSecond = (int) (System.currentTimeMillis() / 1000);

    final Map<Key, Value> db = getDB();

    final long start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      final Key key = new Key();
      key.setLink(1);
      key.setTimestamp(timeSecond - 10);

      final Value value = new Value();
      value.setValue(1);
      value.setScore(2);
      value.setAverage(2);

      db.put(key, value);
    }
    System.out.println(System.currentTimeMillis() - start);
  }

  @SuppressWarnings("all")
  private static Map<Key, Value> getDB() {
    return DBMaker
        .fileDB("D:/mapdb")
        .checksumStoreEnable()
        .fileChannelEnable()
        .fileMmapEnableIfSupported()
        .checksumHeaderBypass()
        .closeOnJvmShutdown()
        .make()
        .hashMap("map", new KeySerializer(), new ValueSerializer())
        .createOrOpen();
  }

  static class KeySerializer implements Serializer<Key>, Serializable {

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull Key key) throws IOException {
      dataOutput2.write(key.getLink());
      dataOutput2.write(key.getTimestamp());
    }

    @Override
    public Key deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      final Key key = new Key();
      key.setLink(dataInput2.readInt());
      key.setTimestamp(dataInput2.readInt());
      return null;
    }
  }

  static class ValueSerializer implements Serializer<Value>, Serializable {

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull Value value)
        throws IOException {
      dataOutput2.writeDouble(value.getValue());
      dataOutput2.writeDouble(value.getScore());
      dataOutput2.writeDouble(value.getAverage());
    }

    @Override
    public Value deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      final Value value = new Value();
      value.setValue(dataInput2.readDouble());
      value.setScore(dataInput2.readDouble());
      value.setAverage(dataInput2.readDouble());
      return value;
    }
  }

  static class Key {
    private int link;
    private int timestamp;

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
