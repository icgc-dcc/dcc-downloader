package org.icgc.dcc.downloader.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import lombok.Value;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

@ThreadSafe
public class ExportedDataFileSystem {

  private final static String DEFAULT_ROOT_DIR = "/icgc/download/static";

  private final static int DEFAULT_BUFFER_SIZE = 32768;

  private final static String SEPARATOR = "/";

  private final FileSystem fs;

  private final Path rootPath;

  private final int bufferSize;

  private final static String CURRENT_DIRECTORY = ".";

  private final CurrentProjectSimLink releaseSymlink;

  public enum AccessPermission {

    UNCHECKED(""), OPEN("open"), CONTROLLED("controlled");

    public String tag;

    AccessPermission(String tag) {
      this.tag = tag;
    }

    public static AccessPermission is(String filename) {
      if (filename.contains(CONTROLLED.tag)) return CONTROLLED;
      if (filename.contains(OPEN.tag)) return OPEN;
      return UNCHECKED;
    }
  }

  @Value
  private static class CurrentProjectSimLink {

    private final String symlink;
    private final String actualPath;

    public String resolveSymlink(String relativePath) {
      if (relativePath.startsWith(symlink)) {
        relativePath =
            StringUtils.replaceOnce(relativePath, symlink,
                actualPath);
      }
      return relativePath;
    }

  }

  public ExportedDataFileSystem(String rootDir, int bufferSize) throws IOException {
    this(rootDir, bufferSize, "/current /dev");
  }

  public ExportedDataFileSystem(String rootDir, String symlink) throws IOException {
    this(rootDir, DEFAULT_BUFFER_SIZE, symlink);
  }

  public ExportedDataFileSystem(String rootDir, int bufferSize, String currentSymlink) throws IOException {

    Configuration conf = new Configuration();
    // Uncomment this if FS is not thread safe
    // conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI rootUri = (new Path(rootDir)).toUri();
    if (rootDir != null) this.rootPath = new Path(rootUri.getPath());
    else
      this.rootPath = new Path(DEFAULT_ROOT_DIR);

    this.bufferSize = bufferSize;
    fs = FileSystem.get(rootUri, conf);
    fs.setWorkingDirectory(rootPath);
    if (!fs.exists(rootPath)) {
      throw new IOException("root directory doesn't exist:" + rootPath);
    }

    String[] currentReleaseLink = currentSymlink.split(" ");
    checkArgument(currentReleaseLink.length == 2, "Invalid argument for currentSymlink:" + currentSymlink);

    releaseSymlink = new CurrentProjectSimLink(currentReleaseLink[0], currentReleaseLink[1]);
  }

  public ExportedDataFileSystem(String rootDir) throws IOException {
    this(rootDir, DEFAULT_BUFFER_SIZE);
  }

  public boolean exists(File relativePath) throws IOException {
    return fs.exists(getDownloadPath(relativePath));
  }

  public long getModificationTime(File relativePath) throws FileNotFoundException, IOException {
    FileStatus status = fs.getFileStatus(getDownloadPath(relativePath));
    return status.getModificationTime();
  }

  public AccessPermission getPermission(File relativePath) throws FileNotFoundException, IOException {
    FileStatus status = fs.getFileStatus(getDownloadPath(relativePath));
    String filename = status.getPath().getName();
    return AccessPermission.is(filename);
  }

  public String getAbsolutePath(File relativePath) {
    return relativePath.getPath();
  }

  public String getName(File relativePath) {
    return relativePath.getName();
  }

  public boolean isDirectory(File relativePath) throws IOException {
    return fs.isDirectory(getDownloadPath(relativePath));
  }

  public boolean isFile(File relativePath) throws IOException {
    return fs.isFile(getDownloadPath(relativePath));
  }

  public List<File> listFiles(File relativePath) throws IOException {
    Builder<File> b = new ImmutableList.Builder<File>();
    // add release to the root
    if (relativePath.getPath().equals(SEPARATOR)) b.add(new File(releaseSymlink.getSymlink()));

    FileStatus[] statuses = fs.listStatus(getDownloadPath(relativePath));
    for (FileStatus status : statuses) {
      b.add(getRelativePathFromAbsolutePath((status.getPath().toUri().getPath())));
    }
    return b.build();
  }

  public long getSize(File relativePath) throws IOException {
    return fs.getFileStatus(getDownloadPath(relativePath)).getLen();
  }

  public String getRootDir() {
    return rootPath.toString();
  }

  public File getRelativePathFromAbsolutePath(String absolutePath) {
    return new File(SEPARATOR, new File(rootPath.toString()).toURI().relativize(new File(absolutePath).toURI())
        .getPath());
  }

  public File normalizedRelativePath(String relativePath) {
    // relativePath = releaseSymlink.resolveSymlink(relativePath);
    File actualPath = new File(rootPath.toString(), relativePath);
    return getRelativePathFromAbsolutePath(actualPath.toString());
  }

  private Path getDownloadPath(File relativePath) {
    relativePath = new File(SEPARATOR,releaseSymlink.resolveSymlink(relativePath.getPath()));
    String pathStr = relativePath.getPath();
    if (SEPARATOR.charAt(0) == pathStr.charAt(0)) pathStr = pathStr.substring(1);
    if (pathStr.equals("")) pathStr = CURRENT_DIRECTORY;
    return new Path(rootPath, pathStr);
  }

  public InputStream createInputStream(File relativePath, long offset) throws IOException {
    // consider rewrite it for inefficiency
    // TODO: https://issues.apache.org/jira/browse/HDFS-246
    Path file = getDownloadPath(relativePath);
    FSDataInputStream fis = fs.open(file, bufferSize);
    try {
      fis.seek(offset);
    } catch (IOException e) {
      // seek fails when the offset requested passes the file length,
      // this line guarantee we are positioned at the end of the file
      IOUtils.skip(fis, offset);
    }
    return fis;
  }

  public void close() throws IOException {
    fs.close();
  }

  public boolean isSymlink(File relativePath) {
    return releaseSymlink.symlink.equals(relativePath.getPath());
  }

}
