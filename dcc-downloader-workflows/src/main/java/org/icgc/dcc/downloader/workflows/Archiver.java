/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.downloader.workflows;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_CONTENT_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.END_OF_LINE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.TSV_DELIMITER;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.DonorIdEncodingUtil;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.CountingOutputStream;
import com.sun.istack.NotNull;

@Slf4j
public class Archiver extends Configured implements Tool {

  private final Configuration config;
  private final AtomicBoolean isCancel;
  private static final int CACHE_SIZE = 1000;
  private static final int QUEUE_SIZE = 10000;
  private static final int BUFFER_SIZE = 65536 * 2;

  public static final String JOB_NAME = "archive-generation";

  private final static String ARCHIVE_SCAN_CACHE_SIZE_PROPERTY = "archive.scan.cache.size";
  private final static String ARCHIVE_WRITER_QUEUE_SIZE_PROPERTY = "archive.writer.queue.size";
  private final static String ARCHIVE_WRITER_BUFFER_SIZE_PROPERTY = "archive.writer.buffer.size";
  private final static String ARCHIVE_OUTPUT_DIR_PROPERTY = "archive.output.dir";
  private final static String ARCHIVE_TTL_PROPERTY = "archive.time.to.live";
  private static final String DOWNLOAD_ID_PROPERTY = "archive.download.id";
  private static final String HEADER_LAST_INDEX_PROPERTY = "header.last.index";
  private static final String ARCHIVE_DATA_TYPE_PROPERTY = "archive.data.type";
  private static final String ARCHIVE_RELEASE_NAME_PROPERTY = "archive.release.name";

  private static final String ENCODED_DONOR_ID_PROPERTY = "encoded.donor.id";

  public Archiver(Configuration config) {
    this.config = config;
    this.isCancel = new AtomicBoolean(true);
  }

  private static class ArchiveMapper extends TableMapper<Text, Text> {

    // effectively final
    private String downloadId;
    private byte headerLastIndex;
    private String dataType;
    private volatile ArchiveTSVWriter writer;
    private String releaseName;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      String id = String.valueOf(context.getTaskAttemptID().getTaskID()
          .getId());
      String outputDir = config.get(ARCHIVE_OUTPUT_DIR_PROPERTY);
      downloadId = config.get(DOWNLOAD_ID_PROPERTY);
      headerLastIndex = Byte.valueOf(config
          .get(HEADER_LAST_INDEX_PROPERTY));
      dataType = config.get(ARCHIVE_DATA_TYPE_PROPERTY);
      releaseName = config.get(ARCHIVE_RELEASE_NAME_PROPERTY);
      if (outputDir != null) {
        writer = new ArchiveTSVWriter(
            new Path(new Path(outputDir), id), config);
      } else {
        writer = new ArchiveTSVWriter(config);
      }

    }

    @Override
    public void map(final ImmutableBytesWritable row, final Result value,
        final Context context) throws InterruptedException, IOException {
      // process data for the row from the Result instance.
      writer.write(value);
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      // any error in output, throw the exception and the job will fail
      // and reexecute by MR framework
      log.info("cleanup...");
      writer.close();
      log.info("Done.");
    }

    private class ArchiveTSVWriter {

      private volatile OutputStream out;
      private volatile CountingOutputStream cos;

      private final ArchiveMetaManager metaManager;
      private final ArrayBlockingQueue<Result> dataQueue;
      private final ExecutorService service;
      private volatile boolean isDone;
      private final Integer queueSize;
      private final Path outputPath;
      private final Configuration config;
      private int bufferSize;
      private boolean isToConsole;
      private int partNumber;

      public ArchiveTSVWriter(Configuration config) throws IOException {
        this.config = config;
        metaManager = new ArchiveMetaManager(SchemaUtil.getMetaTableName(releaseName), config);
        this.queueSize = Integer.valueOf(config.get(
            ARCHIVE_WRITER_QUEUE_SIZE_PROPERTY,
            String.valueOf(QUEUE_SIZE)));
        dataQueue = new ArrayBlockingQueue<Result>(this.queueSize);
        service = Executors.newSingleThreadExecutor();
        System.out.println("Output to stdout for test purposes ...");
        isToConsole = true;
        outputPath = null;
        this.partNumber = 0;
        createOutputStream(partNumber);
      }

      public ArchiveTSVWriter(@NotNull Path outputPath, @NotNull Configuration config)
          throws IOException {
        this.outputPath = outputPath;
        this.config = config;
        metaManager = new ArchiveMetaManager(SchemaUtil.getMetaTableName(releaseName), config);
        this.bufferSize = Integer.valueOf(config.get(
            ARCHIVE_WRITER_BUFFER_SIZE_PROPERTY,
            String.valueOf(BUFFER_SIZE)));
        this.queueSize = Integer.valueOf(config.get(
            ARCHIVE_WRITER_QUEUE_SIZE_PROPERTY,
            String.valueOf(QUEUE_SIZE)));
        log.info("Archive writer buffer size: " + bufferSize + ", queue size: " + queueSize);
        dataQueue = new ArrayBlockingQueue<Result>(queueSize);

        // initialize outputstream
        this.partNumber = 0;
        createOutputStream(partNumber);
        service = Executors.newSingleThreadExecutor();

        service.execute(new Runnable() {

          @Override
          public void run() {
            try {
              while (true) {
                int n = archive();
                if (n == 0) {
                  if (isDone) {
                    // corner case
                    archive();
                    break;
                  } else {
                    // starvation, wait for data...
                    TimeUnit.MILLISECONDS.sleep(500);
                  }
                }
              }
            } catch (Exception e) {
              log.error("reporting failed for download id: "
                  + downloadId + ", data type: " + dataType, e);
              metaManager.resetArchiveStatus(downloadId,
                  dataType);
              throw new RuntimeException(e);
            }
          }
        });
      }

      private void createOutputStream(int partNumber) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path output = new Path(outputPath.getParent(),
            outputPath.getName() + "." + partNumber);
        if (!isToConsole) {
          cos = new CountingOutputStream(fs.create(output, true));
          out = new BufferedOutputStream(new GZIPOutputStream(cos) {

            {
              def.setLevel(Deflater.BEST_SPEED);
            }
          }, bufferSize);
        } else {
          cos = new CountingOutputStream(System.out);
          out = cos;
        }
      }

      private void closeOutputStream() throws IOException {
        try {
          if (out != null) {
            out.close();
          }
        } catch (IOException e) {
          log.error("Fail to close outputstream for TSVWriter", e);
          throw e;
        }
      }

      private int archive() throws IOException {
        // 1. produce tsv lines
        // 2. output it as a part file given the path
        // 3. report status
        ArrayList<Result> values = newArrayListWithCapacity(queueSize);
        int numValues = dataQueue.drainTo(values);
        log.debug("Number of values retrieved: {}", numValues);
        for (val value : values) {
          for (byte i = 0; i <= headerLastIndex; ++i) {
            byte[] v = value.getValue(DATA_CONTENT_FAMILY,
                new byte[] { i });
            if (v != null) {
              out.write(v);
            }
            if (i != headerLastIndex) {
              out.write(TSV_DELIMITER);
            }
          }
          // IOUtils.write(END_OF_LINE, out);
          out.write(END_OF_LINE);

          if (cos.getCount() > ArchiverConstant.MAX_TAR_ENTRY_SIZE_IN_BYTES) {
            closeOutputStream();
            createOutputStream(++partNumber);
          }

          if (downloadId != null) {
            metaManager.reportArchiveStatus(downloadId, dataType,
                1L);
          }
        }
        return numValues;
      }

      public void write(final Result value) throws InterruptedException {
        dataQueue.put(value);
      }

      public void close() throws IOException {
        try {
          isDone = true;
          service.shutdown();
          while (!service.isTerminated()) {
            log.info("waiting for closing writer...");
            TimeUnit.MILLISECONDS.sleep(1000);
          }
          if (dataQueue.size() != 0) {
            log.error("The data queue is not empty before it is closed...");
          }
          closeOutputStream();
        } catch (IOException e) {
          log.error("failed to close. ", e);
          throw e;
        } catch (InterruptedException e) {
          log.error("failed to close due to interrupt. ", e);
        } finally {
          metaManager.close();
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = 0;
    if (args.length > 3) {
      exitCode = 1;
    }
    exitCode = ToolRunner.run(new Archiver(new Configuration()), args);
    System.exit(exitCode);
  }

  public static int mainForTest(String[] args, Configuration conf) throws Exception {
    return ToolRunner.run(new Archiver(conf), args);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) {
    int exitCode = 1;
    HBaseConfiguration.merge(config, getConf());
    String dataType = config.get(ARCHIVE_DATA_TYPE_PROPERTY);
    String releaseName = config.get(ARCHIVE_RELEASE_NAME_PROPERTY);
    String encodedDonorIds = config.get(ENCODED_DONOR_ID_PROPERTY);
    String headerLastIndex = config.get(HEADER_LAST_INDEX_PROPERTY);
    final String downloadId = config.get(DOWNLOAD_ID_PROPERTY);
    final String outputDir = config.get(ARCHIVE_OUTPUT_DIR_PROPERTY);
    int ttl = Integer.valueOf(config.get(ARCHIVE_TTL_PROPERTY));

    ArchiveMetaManager metaManager = null;
    try {
      final FileSystem fs = FileSystem.get(config);
      metaManager = new ArchiveMetaManager(SchemaUtil.getMetaTableName(releaseName), config);
      String tableName = SchemaUtil.getDataTableName(dataType, releaseName);
      createOutputDirectory(fs, outputDir);

      if (headerLastIndex == null) {
        List<String> headers = metaManager.getHeader(dataType);
        headerLastIndex = String.valueOf(headers.size() - 1);
        config.set(HEADER_LAST_INDEX_PROPERTY, headerLastIndex);
      }
      config.set("mapred.map.tasks.speculative.execution", "false");
      config.set("mapred.reduce.tasks.speculative.execution", "false");

      List<Integer> donorIds = DonorIdEncodingUtil.convertCodeToList(encodedDonorIds);
      List<Integer> filteredDonorIds = donorIds;
      if (downloadId != null) {
        filteredDonorIds = metaManager.initialize(dataType, donorIds, downloadId, ttl);
      }

      log.info("Number of donor ids to be retrieved: {}", filteredDonorIds.size());
      Scan scan = createArchiveScan(filteredDonorIds);

      final Job job = new Job(config, JOB_NAME + "(" + tableName + "): " + downloadId);
      job.setJarByClass(Archiver.class); // class that contains mapper
      job.setNumReduceTasks(0);

      TableMapReduceUtil.initTableMapperJob(tableName, // input HBase table
          // name
          scan, // Scan instance to control CF and attribute selection
          ArchiveMapper.class, // mapper
          null, // mapper output key
          null, // mapper output value
          job);
      job.setOutputFormatClass(NullOutputFormat.class);

      exitCode = executeJob(job, fs, new Path(outputDir));

      if (downloadId != null) {
        metaManager.finalizeJobInfo(downloadId, dataType, getFileSize(fs, outputDir));
      }

    } catch (Exception e) {
      log.error("Fail to create archive", e);
    } finally {
      isCancel.set(false);
      if (metaManager != null) {
        metaManager.close();
      }
    }
    return exitCode;
  }

  /**
   * @param encodedDonorIds
   * @return
   */
  private Scan createArchiveScan(List<Integer> donorIds) {
    int firstDonorId = 0;
    int lastDonorId = Integer.MAX_VALUE;
    if (donorIds.size() != 0) {
      firstDonorId = Collections.min(donorIds);
      lastDonorId = Collections.max(donorIds);
    }
    Scan scan = new Scan();
    scan.addFamily(DATA_CONTENT_FAMILY);

    int cacheSize = Integer.valueOf(config.get(
        ARCHIVE_SCAN_CACHE_SIZE_PROPERTY, String.valueOf(CACHE_SIZE)));

    log.info("Scanner cache size: " + cacheSize);

    scan.setCaching(cacheSize);
    scan.setCacheBlocks(false); // don't set to true for MR jobs
    scan.setStartRow(Bytes.add(Bytes.toBytes(firstDonorId),
        Bytes.toBytes(0L)));
    scan.setStopRow(Bytes.add(Bytes.toBytes(lastDonorId),
        Bytes.toBytes(Long.MAX_VALUE)));
    String encodedDonorIds = DonorIdEncodingUtil.encodeIds(ImmutableSet.copyOf(donorIds));
    scan.setFilter(new DonorIDFilter(encodedDonorIds));
    return scan;
  }

  /**
   * @param job
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private int executeJob(final Job job, FileSystem fs, Path outputPath) throws IOException, InterruptedException,
      ClassNotFoundException {
    int exitCode = 1;
    // properly support cancel
    Runtime.getRuntime().addShutdownHook(new Thread()
    {

      @Override
      public void run()
      {
        try {
          if (isCancel.get()) {
            job.killJob();
          }
        } catch (IOException e) {
          log.error("Fail to kill the job: {}", job.getJobID(), e);
        }
      }
    });

    // assume the job cannot finish due to cancellation
    fs.deleteOnExit(outputPath);
    // submit the job and wait
    exitCode = job.waitForCompletion(true) ? 0 : 1;
    // if it finishes, don't delete
    fs.cancelDeleteOnExit(outputPath);
    return exitCode;

  }

  private void createOutputDirectory(FileSystem fs, String outputDir) throws IOException {
    if (outputDir != null) {
      // always recreate the directory
      Path outputPath = new Path(outputDir);
      if (fs.exists(outputPath)) {
        log.warn("Output directory (" + outputPath
            + ") already exists. Deleting...");
        fs.delete(outputPath, true);
      }
      fs.mkdirs(new Path(outputDir));
    }

  }

  private static long getFileSize(FileSystem fs, String outputDir) throws IOException {
    ContentSummary summary = fs.getContentSummary(new Path(outputDir));
    return summary.getLength();
  }
}