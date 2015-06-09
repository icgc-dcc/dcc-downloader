package org.icgc.dcc.downloader.core;

import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_ACTIVE_DOWNLOAD_COUNTER_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_ACTIVE_JOB_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_DATA_TYPE_INFO_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_DOWNLOAD_COUNTER_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_ENCODED_DONOR_IDS_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_END_TIME_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_JOB_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_OOZIE_ID_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_STATS_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_SYSTEM_KEY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TTL_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_USER_EMAIL_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_SIZE_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TYPE_HEADER;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TYPE_INFO_FAMILY;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.icgc.dcc.downloader.core.ArchiveJobSubmitter.ArchiveJobContext;
import org.icgc.dcc.downloader.core.SchemaUtil.JobInfoType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class ArchiveJobManager {

  private final TableResourceManager tableManager;
  private final String oozieUrl;
  private byte capacityThreshold;
  private final static JobStatus EMPTY_JOB_STATUS = new JobStatus(null,
      ImmutableMap.<DataType, JobProgress> of(), true, false);
  private static final JobStatus ERROR_JOB_STATUS = new JobStatus(
      Status.FAILED, ImmutableMap.<DataType, JobProgress> of(), false,
      false);

  public ArchiveJobManager(TableResourceManager tableManager,
      String oozieUrl, byte overCapacityThresholdPercentage) {
    this.tableManager = tableManager;
    this.oozieUrl = oozieUrl;
    this.capacityThreshold = overCapacityThresholdPercentage;
  }

  public Map<String, JobStatus> getStatus(Set<String> downloadIds)
      throws IOException {
    HTable archiveTable = tableManager.getArchiveTable();
    ImmutableMap.Builder<String, JobStatus> allStatusBuilder = ImmutableMap
        .builder();
    try {
      ImmutableList.Builder<Get> statusBuilder = ImmutableList.builder();
      for (String downloadId : downloadIds) {

        Get status = new Get(Bytes.toBytes(downloadId));
        status.addFamily(ARCHIVE_STATS_INFO_FAMILY);
        status.addFamily(ARCHIVE_JOB_INFO_FAMILY);
        statusBuilder.add(status);
      }

      Result[] results = archiveTable.get(statusBuilder.build());

      if (results != null) {
        for (Result result : results) {
          if (!result.isEmpty()) {
            String downloadId = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> jobInfoMap = result
                .getFamilyMap(ARCHIVE_JOB_INFO_FAMILY);
            NavigableMap<byte[], byte[]> statsInfoMap = result
                .getFamilyMap(ARCHIVE_STATS_INFO_FAMILY);

            if (statsInfoMap.isEmpty() || jobInfoMap.isEmpty()) {
              allStatusBuilder.put(downloadId, EMPTY_JOB_STATUS);
              continue;
            }
            ImmutableMap<JobInfoType, ImmutableMap<String, String>> jobInfo = SchemaUtil
                .decodeJobInfo(jobInfoMap);
            ImmutableMap<String, String> info = jobInfo
                .get(JobInfoType.SYSTEM);

            String oozieId = info.get(ARCHIVE_OOZIE_ID_COLUMN);
            Status workflowStatus = getWorkflowStatus(oozieId);
            Map<DataType, JobProgress> progressMap = SchemaUtil
                .decodeStatsInfo(statsInfoMap);
            if (isExpired(jobInfo.get(JobInfoType.CLIENT))) {
              allStatusBuilder.put(downloadId, new JobStatus(
                  workflowStatus, progressMap, false, true));
            } else {

              allStatusBuilder.put(downloadId, new JobStatus(
                  workflowStatus, progressMap, false, false));
            }
          } else {
            log.warn("download Id does not exist: {}",
                Bytes.toString(result.getRow()));
          }
        }
      } else {
        // TODO: need to know how to deal with ids that are not valid
        log.error(
            "Info cannot be found for the list of download ids: {}",
            downloadIds);
      }

    } catch (OozieClientException e) {

      throw new IOException(e);
    } finally {
      tableManager.closeTable(archiveTable);
    }
    return allStatusBuilder.build();
  }

  /**
   * @param immutableMap
   * @return
   */
  boolean isExpired(Map<String, String> info) {
    // when the job has not finished yet, no end time and ttl should be
    // expected
    if (info.containsKey(ARCHIVE_TTL_COLUMN)
        && info.containsKey(ARCHIVE_END_TIME_COLUMN)) {
      long currentTime = System.currentTimeMillis();
      long ttl = Long.valueOf(info.get(ARCHIVE_TTL_COLUMN)) * 3600000L;
      long endTime = Long.valueOf(info.get(ARCHIVE_END_TIME_COLUMN));
      return currentTime > (ttl + endTime);
    }
    return false;
  }

  public String retrieveDownloadId() throws IOException {
    // atomic counter for the number of downloads
    HTable archiveTable = tableManager.getArchiveTable();
    try {
      long counter = archiveTable.incrementColumnValue(
          ARCHIVE_SYSTEM_KEY, ARCHIVE_STATS_INFO_FAMILY,
          ARCHIVE_DOWNLOAD_COUNTER_COLUMN, 1L);
      return getDownloadId(counter);
    } finally {
      tableManager.closeTable(archiveTable);
    }

  }

  private static String getDownloadId(long numOfDownload) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(Bytes.toBytes(numOfDownload));
      return Hex.encodeHexString(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("fail to submit jobs. ", e);
    }
  }

  public Map<DataType, Map<String, Long>> getSizeInfo() throws IOException {
    ImmutableMap.Builder<DataType, Map<String, Long>> resultMapBuilder = ImmutableMap
        .builder();
    HTable metaTable = tableManager.getMetaTable();
    try {

      for (val dataType : DataType.values()) {
        Get dataTypeSizeInfo = new Get(
            Bytes.toBytes(dataType.indexName));
        dataTypeSizeInfo.addFamily(META_SIZE_INFO_FAMILY);
        ImmutableMap.Builder<String, Long> sizeMapBuilder = ImmutableMap
            .builder();
        Result result = metaTable.get(dataTypeSizeInfo);
        if (result != null && !result.isEmpty()) {
          NavigableMap<byte[], byte[]> sizeMapInBytes = result
              .getFamilyMap(META_SIZE_INFO_FAMILY);
          for (val entry : sizeMapInBytes.entrySet()) {
            sizeMapBuilder.put(SchemaUtil.decodeDonorId(entry
                .getKey()),
                SchemaUtil.decodeSizeInfo(entry.getValue())
                    .getTotalSize());
          }
        }
        resultMapBuilder.put(dataType, sizeMapBuilder.build());
      }
    } finally {
      tableManager.closeTable(metaTable);
    }
    return resultMapBuilder.build();
  }

  private Status getWorkflowStatus(String oozieId)
      throws OozieClientException {
    OozieClient cli = new OozieClient(this.oozieUrl);
    WorkflowJob jobInfo = cli.getJobInfo(oozieId);
    return jobInfo.getStatus();
  }

  @Data
  @AllArgsConstructor
  public static class JobStatus {

    Status workflowStatus;
    Map<DataType, JobProgress> progressMap;
    boolean notFound;
    boolean isExpired;
  }

  @Data
  @AllArgsConstructor
  public static class JobProgress {

    long numerator = 0;
    long denominator = 0;

    /**
     * @return
     */
    public boolean isCompleted() {
      if (numerator == 0 || denominator == 0
          || ((float) numerator / denominator) < 1) {
        return false;
      } else {
        return true;
      }
    }

    public float getPercentage() {
      if (denominator == 0) {
        return 0f;
      }
      float percentage = (float) numerator / denominator;
      if (percentage > 1) return 1;
      return percentage;
    }
  }

  /**
   * @param jobContext
   * @param oozieId
   */
  public void recordJobInfo(ArchiveJobContext jobContext, String oozieId) {
    // TODO Auto-generated method stub
    Put jobInfoPut = new Put(Bytes.toBytes(jobContext.getDownloadId()));
    // record job information for later retrieval and analysis
    ImmutableMap.Builder<byte[], byte[]> jobInfoBuilder = ImmutableMap
        .builder();

    jobInfoBuilder.putAll(SchemaUtil.encodeSystemJobInfo(ImmutableMap
        .<String, String> of(ARCHIVE_USER_EMAIL_COLUMN, jobContext
            .getUserEmailAddress(),
            ARCHIVE_ENCODED_DONOR_IDS_COLUMN, jobContext
                .getEncodedDonorIds(),
            ARCHIVE_DATA_TYPE_INFO_COLUMN,
            SchemaUtil.encodeTypeInfo(jobContext
                .getFilterTypeInfo()), ARCHIVE_OOZIE_ID_COLUMN,
            oozieId)));
    jobInfoBuilder.putAll(SchemaUtil.encodeClientJobInfo(jobContext
        .getJobInfo()));
    for (val info : jobInfoBuilder.build().entrySet()) {
      jobInfoPut.add(ARCHIVE_JOB_INFO_FAMILY, info.getKey(),
          info.getValue());
    }

    ImmutableMap.Builder<DataType, JobProgress> statsInfoBuilder = ImmutableMap
        .builder();
    JobProgress initialProgress = new JobProgress(0, 0);

    for (val dataType : jobContext.getDataTypes()) {
      statsInfoBuilder.put(dataType, initialProgress);
    }
    for (val info : SchemaUtil.encodeStatsInfo(statsInfoBuilder.build())
        .entrySet()) {
      jobInfoPut.add(ARCHIVE_STATS_INFO_FAMILY, info.getKey(),
          info.getValue());
    }

    HTable archiveTable = tableManager.getArchiveTable();
    try {
      archiveTable.put(jobInfoPut);
    } catch (IOException e) {
      log.error("fail to record job information: {}",
          jobContext.getDownloadId(), e);
      throw new RuntimeException(e);
    } finally {
      tableManager.closeTable(archiveTable);
    }
  }

  public Map<DataType, List<String>> getHeader(List<DataType> dataTypes)
      throws IOException {
    HTable metaTable = tableManager.getMetaTable();
    try {
      ImmutableList.Builder<Get> headerInfo = ImmutableList.builder();
      for (DataType dataType : dataTypes) {
        Get header = new Get(Bytes.toBytes(dataType.indexName));
        header.addColumn(META_TYPE_INFO_FAMILY, META_TYPE_HEADER);
        headerInfo.add(header);
      }
      Result[] results = metaTable.get(headerInfo.build());

      if (results != null) {
        ImmutableMap.Builder<DataType, List<String>> headerLookupBuilder = ImmutableMap
            .builder();
        for (Result result : results) {
          if (result.isEmpty()
              || !result.containsColumn(META_TYPE_INFO_FAMILY,
                  META_TYPE_HEADER)) {
            log.error("Could not found header information for data type: "
                + Bytes.toString(result.getRow()));
            throw new IOException(
                "Fail to retrieve header information for data type: "
                    + Bytes.toString(result.getRow()));
          } else {
            KeyValue kv = result.getColumnLatest(
                META_TYPE_INFO_FAMILY, META_TYPE_HEADER);
            String[] headers = SchemaUtil.decodeHeader(kv
                .getValue());
            String dataTypeName = Bytes.toString(result.getRow());
            DataType dataType = DataType.valueOf(dataTypeName
                .toUpperCase());
            headerLookupBuilder.put(dataType,
                ImmutableList.copyOf(headers));
          }
        }
        return headerLookupBuilder.build();
      } else {
        RuntimeException e = new RuntimeException(
            "No header information found founds: " + dataTypes);
        log.error("Unable to retrieve header inforamtion on table: {}",
            Bytes.toString(metaTable.getTableName()), e);
        throw e;
      }
    } catch (IOException e) {
      log.error("Could not process header information for data types: "
          + dataTypes, e);
      throw e;

    } finally {
      tableManager.closeTable(metaTable);
    }
  }

  public Map<String, Map<String, String>> getJobInfo(Set<String> downloadIds) {

    HTable archiveTable = tableManager.getArchiveTable();
    ImmutableMap.Builder<String, Map<String, String>> allInfoBuilder = ImmutableMap
        .builder();
    try {
      ImmutableList.Builder<Get> infoRequestBuilder = ImmutableList
          .builder();
      for (String downloadId : downloadIds) {
        Get jobInfo = new Get(Bytes.toBytes(downloadId));
        jobInfo.addFamily(ARCHIVE_JOB_INFO_FAMILY);
        infoRequestBuilder.add(jobInfo);
      }
      Result[] results = archiveTable.get(infoRequestBuilder.build());
      if (results != null) {
        for (Result result : results) {
          if (!result.isEmpty()) {
            NavigableMap<byte[], byte[]> jobInfoMap = result
                .getFamilyMap(ARCHIVE_JOB_INFO_FAMILY);
            ImmutableMap<JobInfoType, ImmutableMap<String, String>> jobInfo = SchemaUtil
                .decodeJobInfo(jobInfoMap);

            String downloadId = Bytes.toString(result.getRow());
            allInfoBuilder.put(downloadId,
                jobInfo.get(JobInfoType.CLIENT));

          } else {
            log.warn("download Id does not exist: {}",
                Bytes.toString(result.getRow()));
          }
        }
      } else {
        // TODO: need to know how to deal with ids that are not valid
        log.error(
            "Info cannot be found for the list of download ids: {}",
            downloadIds);
      }
    } catch (Exception e) {
      log.error("Cannot found job info for download ids: {}",
          downloadIds, e);
    } finally {
      tableManager.closeTable(archiveTable);
    }
    return allInfoBuilder.build();
  }

  public JobStatus cancel(String downloadId) {
    HTable archiveTable = tableManager.getArchiveTable();
    try {
      Get get = new Get(Bytes.toBytes(downloadId));
      get.addFamily(ARCHIVE_STATS_INFO_FAMILY);
      get.addColumn(ARCHIVE_JOB_INFO_FAMILY,
          SchemaUtil.encodeSystemJobInfoKey(ARCHIVE_OOZIE_ID_COLUMN));
      Result result = archiveTable.get(get);

      if (result == null || result.isEmpty()) {
        log.warn("Fail to cancel because download id not found: {} ",
            downloadId);
        // NOT EMPTY, ERROR_JOB_STATUS
        return ERROR_JOB_STATUS;
      } else {
        ImmutableMap<JobInfoType, ImmutableMap<String, String>> jobInfoMap = SchemaUtil
            .decodeJobInfo(result
                .getFamilyMap(ARCHIVE_JOB_INFO_FAMILY));
        NavigableMap<byte[], byte[]> statsInfoMap = result
            .getFamilyMap(ARCHIVE_STATS_INFO_FAMILY);

        if (statsInfoMap.isEmpty() || jobInfoMap.isEmpty()) return ERROR_JOB_STATUS;
        String oozieId = jobInfoMap.get(JobInfoType.SYSTEM).get(
            ARCHIVE_OOZIE_ID_COLUMN);
        OozieClient cli = new OozieClient(this.oozieUrl);
        cli.kill(oozieId);
        Map<DataType, JobProgress> progressMap = SchemaUtil
            .decodeStatsInfo(statsInfoMap);
        return new JobStatus(Status.KILLED, progressMap, false, false);
      }

    } catch (Exception e) {
      log.error("Fail to cancel job with id {}: ", downloadId, e);
    } finally {
      tableManager.closeTable(archiveTable);
    }
    return ERROR_JOB_STATUS;
  }

  public void flagActiveJob(String downloadId) {
    recordCurrentNumberOfDownload(downloadId, 1);
  }

  public void flagInactiveJob(String downloadId) {
    recordCurrentNumberOfDownload(downloadId, -1);
  }

  private void recordCurrentNumberOfDownload(String downloadId,
      long numOfDownload) {
    HTable archiveTable = tableManager.getArchiveTable();
    try {
      archiveTable.incrementColumnValue(Bytes.toBytes(downloadId),
          ARCHIVE_ACTIVE_JOB_FAMILY,
          ARCHIVE_ACTIVE_DOWNLOAD_COUNTER_COLUMN, numOfDownload);
    } catch (IOException e) {
      log.error("fail to increment the download counter for ID: {}",
          downloadId, e);
    } finally {
      tableManager.closeTable(archiveTable);
    }
  }

  public boolean isServiceAvailable() {
    try {
      OozieClient cli = new OozieClient(this.oozieUrl);
      SYSTEM_MODE mode = cli.getSystemMode();
      if (SYSTEM_MODE.NORMAL != mode) return false;
      return true;
    } catch (OozieClientException e) {
      log.error("cannot connect to oozie", e);
      return false;
    }
  }

  // only allow download if storage has at least FREE_DISK_SPACE_PERCENTAGE of
  // free disk space
  public boolean isOverCapacity() {
    try {
      if (capacityThreshold > tableManager.getFreeDiskSpacePercentage()) {
        log.warn("Threshold limit reached for storage: {}",
            capacityThreshold);
        log.warn("Running low in disk space, only left (%): {}",
            tableManager.getFreeDiskSpacePercentage());
        return true;
      }
    } catch (IOException e) {
      // if there is an error, report it as over capacity
      log.error("Over capacity triggered", e);
    }
    return false;
  }

  public String getReleaseName() {
    return tableManager.getReleaseName();
  }
}
