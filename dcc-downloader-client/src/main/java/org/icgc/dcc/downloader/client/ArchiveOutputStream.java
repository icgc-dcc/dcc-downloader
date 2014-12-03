package org.icgc.dcc.downloader.client;

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.downloader.core.ArchiverConstant.END_OF_LINE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.TSV_DELIMITER;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.DataType;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

@Slf4j
@RequiredArgsConstructor
class ArchiveOutputStream {

  @NonNull
  private final Path dynamicDownloadPath;
  @NonNull
  private final Configuration conf;

  private static final String EXTENSION = ".tsv.gz";
  private final static long MAX_BUCKET_SIZE = 4294967296L; // 4 GB per tar

  public boolean streamArchiveInGzTar(OutputStream out, String downloadId,
      List<DataType> downloadedDataTypes, Map<DataType, List<String>> headersLookupMap) throws IOException {
    try {
      FileSystem fs = FileSystem.get(dynamicDownloadPath.toUri(), conf);
      Path downloadPath = new Path(dynamicDownloadPath, downloadId);
      FileStatus[] files = fs.listStatus(downloadPath);

      if (files == null) return false;

      ImmutableList.Builder<List<PathBucket>> builder = ImmutableList.builder();
      for (val dataType : downloadedDataTypes) {
        Path downloadTypePath = new Path(downloadPath, dataType.indexName);
        if (fs.exists(downloadTypePath)) {
          // the directory name is the data type index name

          log.info("Trying to download data for download id : "
              + downloadId + ", Data Type: " + dataType);
          builder.add(bucketize(fs, downloadTypePath,
              dataType, headersLookupMap.get(dataType)));
        }
      }
      @Cleanup
      TarArchiveOutputStream tarOut = new TarArchiveOutputStream(
          new BufferedOutputStream(out));
      List<List<PathBucket>> entryDataTypes = builder.build();
      for (val tarEntryInputs : entryDataTypes) {
        boolean isMultiPart = (tarEntryInputs.size() > 1) ? true : false;
        for (val bucket : tarEntryInputs) {
          byte[] header = gzipHeader(Bytes.toBytes(bucket.getHeader()));

          String extension = (isMultiPart) ? ".part-" + bucket.getBucketNumber() + EXTENSION : EXTENSION;
          addArchiveEntry(tarOut, bucket.getBucketName() + extension,
              bucket.getSize() + header.length);
          concatGZipFiles(fs, tarOut, bucket.getPaths(), header);
          closeArchiveEntry(tarOut);
        }
      }
      return true;
    } catch (Exception e) {
      log.error("Fail to produce an archive for download id: "
          + downloadId + ", download data Types: " + downloadedDataTypes, e);
    }
    return false;
  }

  private byte[] gzipHeader(byte[] header) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    GZIPOutputStream gout = new GZIPOutputStream(bos) {

      {
        def.setLevel(Deflater.BEST_SPEED);
      }
    };

    gout.write(header);
    gout.write(END_OF_LINE);
    gout.close();
    return bos.toByteArray();
  }

  public boolean streamArchiveInGz(OutputStream out, String downloadId,
      DataType dataType, Map<DataType, List<String>> headerLookupMap) throws IOException {
    try {
      FileSystem fs = FileSystem.get(dynamicDownloadPath.toUri(), conf);
      Path dataTypePath = new Path(dynamicDownloadPath, new Path(
          downloadId, dataType.indexName));
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(
          dataTypePath, false);
      List<Path> paths = newArrayList();

      while (files.hasNext()) {
        paths.add(files.next().getPath());
      }

      @Cleanup
      OutputStream managedOut = out;
      concatGZipFiles(
          fs,
          managedOut,
          paths,
          gzipHeader(Bytes.toBytes(formatHeader(headerLookupMap.get(dataType)))));
      return true;
    } catch (Exception e) {
      log.error("Fail to stream archive. DownloadID: " + downloadId, e);
    }
    return false;
  }

  private String formatHeader(List<String> headers) {
    return Joiner.on(Bytes.toString(TSV_DELIMITER)).join(headers);
  }

  private List<PathBucket> bucketize(FileSystem fs, Path dataTypePath,
      DataType allowedType, List<String> headers) throws IOException {

    // First-Fit Algorithm
    String header = formatHeader(headers);
    List<PathBucket> buckets = newArrayList();
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(dataTypePath,
        false);
    int bucketNum = 1;
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      boolean isFound = false;
      if (file.getLen() > MAX_BUCKET_SIZE) {
        log.error("File size too big to fit into a single bucket (Tar Entry Limitation): "
            + file.getPath());
        throw new RuntimeException(
            "Cannot tar the entry with file size exceeding the limit :"
                + file.getPath());
      } else {
        for (val bucket : buckets) {
          if ((bucket.getSize() + file.getLen()) < MAX_BUCKET_SIZE) {
            bucket.add(file.getLen(), file.getPath());
            isFound = true;
            continue;
          }
        }
        if (!isFound) {
          PathBucket newBucket = new PathBucket(
              dataTypePath.getName(), bucketNum,
              header, file.getLen(), newArrayList(file.getPath()));
          buckets.add(newBucket);
          bucketNum++;
        }
      }
    }

    return buckets;

  }

  private void concatGZipFiles(FileSystem fs, OutputStream out,
      List<Path> files, byte[] header) throws IOException {
    IOUtils.write(header, out);
    for (val file : files) {
      FSDataInputStream is = fs.open(file);
      try {
        IOUtils.copy(is, out);
      } finally {
        is.close();
      }
    }
  }

  @SneakyThrows
  private static void addArchiveEntry(TarArchiveOutputStream os,
      String filename, long fileSize) {
    TarArchiveEntry entry = new TarArchiveEntry(filename);
    entry.setSize(fileSize);
    os.putArchiveEntry(entry);
  }

  @SneakyThrows
  private static void closeArchiveEntry(TarArchiveOutputStream os) {
    os.closeArchiveEntry();
  }

  @Data
  @AllArgsConstructor
  private static class PathBucket {

    String bucketName;
    int bucketNumber;
    String header;
    long size;
    List<Path> paths;

    public void add(long size, Path path) {
      this.size = this.size + size;
      paths.add(path);
    }
  }
}
