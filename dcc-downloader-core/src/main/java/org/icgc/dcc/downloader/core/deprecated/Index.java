package org.icgc.dcc.downloader.core.deprecated;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

@Slf4j
public class Index {

  private static final String idx = "download.index";

  @SneakyThrows
  public static void main(String[] argv) {

    Path downloadPath = new Path(argv[0]);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (fs.exists(downloadPath)) {

      Path idxPath = new Path(downloadPath, idx);
      if (fs.exists(idxPath)) {
        fs.delete(idxPath, false);
      }

      FileStatus[] statuses = fs.listStatus(downloadPath);
      // create out after listing
      @Cleanup
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
          fs.create(idxPath), Charsets.US_ASCII));
      for (FileStatus status : statuses) {
        String filename = status.getPath().getName();
        long filesize = status.getLen();
        out.write(filename + "\t" + filesize);
        out.newLine();
      }
    } else
      log.error("Dynamic download path does not exist: {}", downloadPath);
  }
}
