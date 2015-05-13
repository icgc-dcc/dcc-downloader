/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.downloader.core;

import static com.google.common.collect.Maps.newHashMap;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_JOB_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_STATS_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TABLE_NAME;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TTL_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.HEADER_SEPARATOR;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_SIZE_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TABLE_NAME;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TYPE_HEADER;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TYPE_INFO_FAMILY;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.DataType;
import org.icgc.dcc.downloader.core.SchemaUtil;
import org.icgc.dcc.downloader.core.SizeInfo;
import org.icgc.dcc.downloader.core.ArchiveJobManager.JobProgress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

/**
 * ArchiveMetaManager maintains the meta information during archiving process
 */
@Slf4j
public class ArchiveMetaManager {

  private static final int INCREMENT_DELAY_CYCLE = 5;
  private final HTable metaTable;
  private final HTable archiveTable;
  private final ScheduledExecutorService executor;
  private final Map<String, byte[]> lookupMap;

  private final AtomicLong runningIncrement;
  private long runningTotalCount;

  private static final String META_TABLE_NAME_PROPERTY = "archive.meta.table.name";

  public ArchiveMetaManager(Configuration config) throws IOException {
    String metaTablename = config.get(META_TABLE_NAME_PROPERTY);
    metaTable = (metaTablename == null) ? new HTable(config, META_TABLE_NAME) : new HTable(config, metaTablename);
    archiveTable = new HTable(config, ARCHIVE_TABLE_NAME);
    archiveTable.setAutoFlush(false);
    metaTable.setAutoFlush(false);
    lookupMap = newHashMap();
    runningIncrement = new AtomicLong(0);
    runningTotalCount = 0;
    executor = Executors.newSingleThreadScheduledExecutor();
  }

  public ArchiveMetaManager(String metaTablename, Configuration config) throws IOException {
    metaTable = new HTable(config, metaTablename);
    archiveTable = new HTable(config, ARCHIVE_TABLE_NAME);
    archiveTable.setAutoFlush(false);
    metaTable.setAutoFlush(false);
    lookupMap = newHashMap();
    runningIncrement = new AtomicLong(0);
    runningTotalCount = 0;
    executor = Executors.newSingleThreadScheduledExecutor();
  }

  public List<Integer> initialize(String dataType, List<Integer> donorIds, String downloadId, int ttl)
      throws IOException {
    Get getDonorSizeInfo = new Get(Bytes.toBytes(dataType));
    for (int id : donorIds) {
      getDonorSizeInfo.addColumn(META_SIZE_INFO_FAMILY, Bytes.toBytes(id));
    }

    log.info("Initializing archiving job with stats for data type: " + dataType);
    Result result = metaTable.get(getDonorSizeInfo);
    NavigableMap<byte[], byte[]> sizeInfoMap = result.getFamilyMap(META_SIZE_INFO_FAMILY);
    if (sizeInfoMap == null) {
      log.error("No donor information found for download id: " + downloadId);
      throw new RuntimeException("Please check with the integrity of the data for download id: " + downloadId);
    }
    long totalLine = 0;
    Builder<Integer> filteredId = ImmutableList.builder();
    for (val kv : sizeInfoMap.entrySet()) {
      filteredId.add(Bytes.toInt(kv.getKey()));
      byte[] size = kv.getValue();
      SizeInfo sizeInfo = SchemaUtil.decodeSizeInfo(size);
      totalLine = totalLine + sizeInfo.getTotalLine();
    }

    Map<DataType, JobProgress> statsInfoMap =
        ImmutableMap.of(DataType.valueOf(dataType.toUpperCase()),
            new JobProgress(0L, totalLine));
    Map<byte[], byte[]> encodedStatsInfoMap = SchemaUtil.encodeStatsInfo(statsInfoMap);
    Put put = new Put(Bytes.toBytes(downloadId));
    for (val entry : encodedStatsInfoMap.entrySet()) {
      put.add(ARCHIVE_STATS_INFO_FAMILY, entry.getKey(), entry.getValue());
    }

    Map<byte[], byte[]> clientInfo =
        SchemaUtil.encodeClientJobInfo(ImmutableMap.<String, String> of(ARCHIVE_TTL_COLUMN, String.valueOf(ttl)));
    for (val entry : clientInfo.entrySet()) {
      put.add(ARCHIVE_JOB_INFO_FAMILY, entry.getKey(), entry.getValue());
    }

    archiveTable.put(put);
    archiveTable.flushCommits();

    return filteredId.build();
  }

  public void keepMetaInfo(String dataType, int id, long totalLine, long totalSize) throws IOException {
    keepMetaInfo(dataType, id, totalLine, totalSize, null);
  }

  public void keepMetaInfo(String dataType, int id, long totalLine, long totalSize, String[] headers)
      throws IOException {
    Put metaPut = new Put(Bytes.toBytes(dataType));
    metaPut.add(META_SIZE_INFO_FAMILY, Bytes.toBytes(id),
        SchemaUtil.encodeSizeInfo(new SizeInfo(totalLine, totalSize)));
    if (headers != null) {
      metaPut.add(META_TYPE_INFO_FAMILY, META_TYPE_HEADER, SchemaUtil.encodeHeader(headers));
    }
    metaTable.put(metaPut);
  }

  public void addHeader(String dataType, String[] headers) throws IOException {
    Put metaPut = new Put(Bytes.toBytes(dataType));
    if (headers != null) {
      metaPut.add(META_TYPE_INFO_FAMILY, META_TYPE_HEADER, SchemaUtil.encodeHeader(headers));
    }
    metaTable.put(metaPut);
  }

  public List<String> getHeader(String dataType) throws IOException {
    Builder<String> builder = ImmutableList.builder();

    Get get = new Get(Bytes.toBytes(dataType));
    get.addFamily(META_TYPE_INFO_FAMILY);
    get.addColumn(META_TYPE_INFO_FAMILY, META_TYPE_HEADER);

    Result result;
    try {
      result = metaTable.get(get);
    } catch (IOException e) {
      log.error("Fail to retrive header information for datatype: " + dataType, e);
      throw e;
    }

    String header = Bytes.toString(result.getValue(META_TYPE_INFO_FAMILY, META_TYPE_HEADER));
    if (header != null) {
      builder.add(header.split(HEADER_SEPARATOR));
    } else {
      throw new RuntimeException("No header information found for data type: " + dataType);
    }
    return builder.build();
  }

  public void resetArchiveStatus(final String downloadId, final String dataType) {

    try {
      archiveTable.incrementColumnValue(lookup(downloadId), ARCHIVE_STATS_INFO_FAMILY, lookup(dataType),
          -runningTotalCount,
          false);

    } catch (IOException e) {
      log.error("Fail to reset status");
    } finally {
      runningTotalCount = 0;
    }
  }

  public long reportArchiveStatus(final String downloadId, final String dataType, final long incrementAmount)
      throws IOException {
    runningTotalCount = runningTotalCount + incrementAmount;

    runningIncrement.addAndGet(incrementAmount);
    executor.schedule(new Runnable() {

      @Override
      public void run() {
        long increment = runningIncrement.get();
        if (increment != 0) {
          runningIncrement.addAndGet(-increment);
          try {
            archiveTable.incrementColumnValue(lookup(downloadId), ARCHIVE_STATS_INFO_FAMILY, lookup(dataType),
                increment,
                false);
          } catch (IOException e) {
            // undo
            runningIncrement.addAndGet(increment);
            log.error("Unable to write statistics. Try next cycle...");
          }
        }
      }
    }, INCREMENT_DELAY_CYCLE, TimeUnit.SECONDS);

    return runningTotalCount;
  }

  public void close() {
    try {
      archiveTable.close();
    } catch (Exception e) {
      log.error("fail to close archive table object");
    }
    try {
      metaTable.close();
    } catch (Exception e) {
      log.error("fail to close meta table object");
    }
    executor.shutdown();
    try {
      // wait one more cycle
      executor.awaitTermination(INCREMENT_DELAY_CYCLE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("fail to shutdown properly.");
    }
  }

  private byte[] lookup(String text) {
    byte[] bytes = lookupMap.get(text);
    if (bytes == null) {
      bytes = Bytes.toBytes(text);
      lookupMap.put(text, bytes);
    }
    return bytes;
  }

  public void finalizeJobInfo(String downloadId, String dataType, long fileSize) throws IOException {
    Put put = new Put(Bytes.toBytes(downloadId));
    Map<byte[], byte[]> jobInfo = SchemaUtil.encodeClientJobInfo(ImmutableMap.<String, String> of(
        ArchiverConstant.ARCHIVE_END_TIME_COLUMN, String.valueOf(System.currentTimeMillis())
        ));
    for (val entry : jobInfo.entrySet()) {
      put.add(ARCHIVE_JOB_INFO_FAMILY, entry.getKey(), entry.getValue());
    }
    archiveTable.put(put);

    Map<byte[], byte[]> sizeInfo = SchemaUtil.encodeClientJobInfo(ImmutableMap.<String, String> of(
        ArchiverConstant.ARCHIVE_FILE_SIZE_COLUMN, String.valueOf(fileSize)
        ));

    archiveTable.incrementColumnValue(lookup(downloadId), ARCHIVE_JOB_INFO_FAMILY, sizeInfo.keySet().iterator().next(),
        fileSize,
        true);

  }
}
