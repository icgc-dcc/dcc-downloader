package org.icgc.dcc.downloader.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.regex.Pattern;

import lombok.AllArgsConstructor;

import org.apache.hadoop.io.Text;

public class Utils {

  public static final String KEY_SEPARATOR = ".";
  public static final String DELIM_SEPARATOR = "\t";
  public static final int NUM_PARTS_COMPOSITE_KEY = 6;

  public enum STREAM_STATE {
    START(0), SYNC(1), END(2);

    private int id;

    STREAM_STATE(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }

  }

  private Utils() {
    throw new AssertionError();
  }

  public static CompositeKey getCompositeKey(Text key) {
    checkNotNull(key);
    return getCompositeKey(key.toString());
  }

  public static CompositeKey getCompositeKey(String key) {
    String[] info = key.split(Pattern.quote(KEY_SEPARATOR));
    checkArgument(info.length == NUM_PARTS_COMPOSITE_KEY,
        "Skipping this key because it does not conform to the key structure: "
            + key);
    String project = info[0];
    String donorId = info[1];
    String fileType = info[2];
    String dataType = info[3];
    int state = Integer.parseInt(info[4]);
    long size = Long.parseLong(info[5]);
    return new CompositeKey(project, donorId, fileType, dataType, size,
        state);
  }

  @AllArgsConstructor
  public static class CompositeKey {

    public String project;
    public String donorId;
    public String fileType;
    public String dataType;
    public long size;
    public int state;

  }
}
