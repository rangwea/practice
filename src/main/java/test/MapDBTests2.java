package test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import test.LevelDBTests.Key;
import test.LevelDBTests.Value;

/**
 * @author wufj(feijian_wu@kingdee.com)
 * @since 2019/1/3 22:34
 */
@SuppressWarnings("all")
public class MapDBTests2 {

  public static void main(String[] args) {
    //testRead();//80
    testWrite();//82,5448
  }

  private static void testRead() {
    final Map<byte[], byte[]> db = getDB();

    AtomicInteger counter = new AtomicInteger();
    final long start = System.currentTimeMillis();
    db.forEach((k, v) -> {
      counter.getAndIncrement();
    });
    System.out.println(counter.get());
    System.out.println(System.currentTimeMillis() - start);
  }

  private static void testWrite() {
    int timeSecond = (int) (System.currentTimeMillis() / 1000);

    final Map<byte[], byte[]> db = getDB();

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
  private static Map<byte[], byte[]> getDB() {
    return DBMaker
        .fileDB("D:/mapdb")
        .checksumStoreEnable()
        .fileChannelEnable()
        .fileMmapEnableIfSupported()
        .checksumHeaderBypass()
        .closeOnJvmShutdown()
        .make()
        .hashMap("map", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
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

    public MapDBTests2.Key setLink(int link) {
      this.link = link;
      return this;
    }

    public int getTimestamp() {
      return timestamp;
    }

    public MapDBTests2.Key setTimestamp(int timestamp) {
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

    public MapDBTests2.Value setValue(double value) {
      this.value = value;
      return this;
    }

    public double getScore() {
      return score;
    }

    public MapDBTests2.Value setScore(double score) {
      this.score = score;
      return this;
    }

    public double getAverage() {
      return average;
    }

    public MapDBTests2.Value setAverage(double average) {
      this.average = average;
      return this;
    }
  }
}
