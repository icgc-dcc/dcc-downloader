package org.icgc.dcc.downloader.client;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;

@Slf4j
public class TarGzDownload {

  @SneakyThrows
  public static void build(FileSystem fs, Path inputPath, Path outputPath) {
    List<Path> inputPaths = Lists.newArrayList();
    if (fs.isDirectory(inputPath)) {
      FileStatus[] statuses = fs.listStatus(inputPath);

      for (FileStatus status : statuses) {
        if (status.isFile()) {
          inputPaths.add(status.getPath());
        }
      }
    } else

      inputPaths.add(inputPath);

    build(fs, inputPaths, outputPath);
  }

  @SneakyThrows
  public static void build(FileSystem fs, List<Path> inputPaths, Path outputPath) {
    build(fs, inputPaths, fs.create(outputPath));
  }

  @SneakyThrows
  public static void build(FileSystem fs, List<Path> inputPaths, OutputStream os) {
    Configuration conf = new Configuration();
    TarArchiveOutputStream tarOut = null;
    try {
      tarOut = new TarArchiveOutputStream(new GZIPOutputStream(
          new BufferedOutputStream(os)));

      for (Path path : inputPaths) {
        if (fs.isDirectory(path)) log.warn("Path ({}) is a directory, omitting", path);
        else {

          TarArchiveEntry entry = new TarArchiveEntry(path.getName());
          entry.setSize(fs.getFileStatus(path).getLen());
          tarOut.putArchiveEntry(entry);

          FSDataInputStream in = null;
          try {
            in = fs.open(path);
            IOUtils.copyBytes(in, tarOut, conf, false);
          } finally {
            IOUtils.closeStream(in);
          }
          tarOut.closeArchiveEntry();
        }
      }
    } finally {
      if (tarOut != null) {
        tarOut.finish();
        IOUtils.closeStream(tarOut);
      } else
        log.error("fail to close output stream because it is null.");
    }
  }

  public static class Options {

    // call this archive later
    @Parameter(names = { "-i", "--inputDir" }, required = false, description = "Directory containing the input files")
    public String inputDir = "/icgc/download/dynamic";

    // call this dynamic later
    @Parameter(names = { "-o", "--outputDir" }, required = false, description = "Output directory")
    public String outputDir = "/icgc/download/build";

    @Parameter(names = { "-f", "--filename" }, required = false, description = "The file names inside the input directory. If omitted, all files in the directory will be included")
    public List<String> filenames = new ArrayList<String>();
  }

  private static final Options options = new Options();

  @SneakyThrows
  public static void main(String[] args) {
    JCommander cli = new JCommander(options);
    cli.parse(args);

    Path outputPath = new Path(options.outputDir);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (options.filenames.size() == 0) {

      build(fs, new Path(options.inputDir), outputPath);

    } else {

      List<Path> inputPaths = new ArrayList<Path>(options.filenames.size());
      Path parentPath = new Path(options.inputDir);
      for (String filename : options.filenames) {
        inputPaths.add(new Path(parentPath, filename));
      }

      build(fs, inputPaths, outputPath);

    }
  }
}
