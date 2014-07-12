package ict.wde.hbase.tpcc;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;

public class Utils {

  static final char[] STRING_CHARSET = "_- abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
      .toCharArray();
  static final char[] LETTER_CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
      .toCharArray();
  static final char[] DIGIT_CHARSET = "0123456789".toCharArray();

  private static Random rand() {
    return new Random();
  }

  public static int random(int x, int y) {
    return random(rand(), x, y);
  }

  public static int random(Random rand, int x, int y) {
    return x + rand.nextInt(y - x + 1);
  }

  static char randChar(Random rand) {
    return STRING_CHARSET[rand.nextInt(STRING_CHARSET.length)];
  }

  static char randLetterChar(Random rand) {
    return LETTER_CHARSET[rand.nextInt(LETTER_CHARSET.length)];
  }

  static char randDigitChar(Random rand) {
    return DIGIT_CHARSET[rand.nextInt(DIGIT_CHARSET.length)];
  }

  public static String randomLetterString(int x, int y) {
    Random rand = rand();
    int length = random(rand, x, y);
    StringBuffer sb = new StringBuffer(length);
    for (int i = 0; i < length; ++i) {
      sb.append(randLetterChar(rand));
    }
    return sb.toString();
  }

  public static String randomDigitString(int x, int y) {
    Random rand = rand();
    int length = random(rand, x, y);
    StringBuffer sb = new StringBuffer(length);
    for (int i = 0; i < length; ++i) {
      sb.append(randDigitChar(rand));
    }
    return sb.toString();
  }

  public static String randomString(int x, int y) {
    Random rand = rand();
    int length = random(rand, x, y);
    StringBuffer sb = new StringBuffer(length);
    for (int i = 0; i < length; ++i) {
      sb.append(randChar(rand));
    }
    return sb.toString();
  }

  public static byte[] randomZip() {
    Random rand = rand();
    return String.format("%04d11111", rand.nextInt(10000)).getBytes();
  }

  public static int randomDid() {
    return Utils.random(0, 9);
  }

  public static int randomWidExcept(int w_id) {
    int ret;
    do {
      ret = Utils.random(0, Const.W - 1);
      if (Const.W == 1) break;
    }
    while (ret == w_id);
    return ret;
  }

  public static byte[] currentTimestamp() {
    return Bytes.toBytes(System.currentTimeMillis());
  }

  public static byte[] concat(byte[]... arrays) {
    int length = 0;
    for (byte[] array : arrays) {
      length += array.length;
    }
    int offset = 0;
    byte[] ret = new byte[length];
    for (byte[] array : arrays) {
      offset = Bytes.putBytes(ret, offset, array, 0, array.length);
    }
    return ret;
  }

  static final String[] LAST_SYLLABLE = { "BAR", "OUGHT", "ABLE", "PRI",
      "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };

  public static String lastName(int number) {
    int p3 = number % 10;
    number /= 10;
    int p2 = number % 10;
    number /= 10;
    int p1 = number % 10;
    return new StringBuffer(15).append(LAST_SYLLABLE[p1])
        .append(LAST_SYLLABLE[p2]).append(LAST_SYLLABLE[p3]).toString();
  }

  public static int NURand_C_LAST() {
    return NURand(//
        Const.A_C_LAST, //
        Const.C_C_LAST, //
        Const.X_C_LAST, //
        Const.Y_C_LAST);
  }

  public static int NURand_C_ID() {
    return NURand(//
        Const.A_C_ID, //
        Const.C_C_ID, //
        Const.X_C_ID, //
        Const.Y_C_ID);
  }

  public static int NURand_OL_I_ID() {
    return NURand(//
        Const.A_OL_I_ID, //
        Const.C_OL_I_ID, //
        Const.X_OL_I_ID, //
        Const.Y_OL_I_ID);
  }

  // (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
  public static int NURand(int A, int C, int x, int y) {
    Random rand = rand();
    return (((random(rand, 0, A) | random(rand, x, y)) + C) % (y - x + 1)) + x;
  }

  public static void sleep(long millSec) {
    try {
      Thread.sleep(millSec);
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  public static byte[] n2b(long num) {
    return Bytes.toBytes(num);
  }

  public static long b2n(byte[] bytes) {
    return Bytes.toLong(bytes);
  }

  private static final String SPACE10 = "          ";

  public static String space(int n) {
    StringBuffer sb = new StringBuffer(n);
    appendSpace(n, sb);
    return sb.toString();
  }

  public static void appendSpace(int n, StringBuffer sb) {
    int div = n / 10;
    int mod = n % 10;
    for (int i = 0; i < div; ++i)
      sb.append(SPACE10);
    for (int i = 0; i < mod; ++i)
      sb.append(' ');
  }

  public static long thinkingTime(long u) {
    double r;
    do {
      r = Math.random();
    }
    while (r == 0.0);
    return (long) (-Math.log(r) * u * 1000);
  }

  public static String timeToStringMinute(long time) {
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM", time);
  }

  public static String timeToStringSecond(long time) {
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", time);
  }

  public static String timeToStringDuration(long time) {
    time /= 1000;
    long sec = time % 60;
    time /= 60;
    long min = time % 60;
    time /= 60;
    return String.format("%02d:%02d:%02d", time, min, sec);
  }

  public static void main(String[] args) {
    for (int i = 0; i < 10000; ++i) {
      System.out.println(thinkingTime(20));
    }
  }
}
