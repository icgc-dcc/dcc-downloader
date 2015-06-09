package org.icgc.dcc.downloader.core;

import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_DOWNLOAD_ID_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_ENCODED_DONOR_IDS_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_OUTPUT_DIR_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_RELEASE_NAME_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_STATUS_URL_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_SUPPORT_EMAIL_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_USER_EMAIL_PROPERTY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.WORKFLOW_USER_NAME_PROPERTY;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.Data;
import lombok.val;
import lombok.Builder;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;

public class ArchiveJobSubmitter {

  public static final String DEFAULT_OOZIE_URL = "http://localhost:11000/oozie";
  public static final String DEFAULT_SUPPORT_EMAIL_ADDRESS = "***REMOVED***";
  public static final String DEFAULT_APP_PATH =
      "hdfs://***REMOVED***:8020/user/downloader/workflows/archive-main";

  private final String supportEmailAddress;
  private final String oozieUrl;
  private final String appPath;

  public ArchiveJobSubmitter() {
    this(DEFAULT_SUPPORT_EMAIL_ADDRESS);
  }

  public ArchiveJobSubmitter(String supportEmailAddress) {
    this(DEFAULT_OOZIE_URL, DEFAULT_APP_PATH, supportEmailAddress);
  }

  public ArchiveJobSubmitter(String oozieUrl, String appPath,
      String supportEmailAddress) {
    this.oozieUrl = oozieUrl;
    this.appPath = appPath;
    this.supportEmailAddress = supportEmailAddress;
  }

  public String getSupportEmailAddress() {
    return this.supportEmailAddress;
  }

  public String submit(ArchiveJobContext job) throws OozieClientException {
    OozieClient cli = new OozieClient(oozieUrl);
    Properties conf = cli.createConfiguration();
    conf.setProperty(WORKFLOW_USER_NAME_PROPERTY, "downloader");

    for (val dataType : job.dataTypes) {
      conf.setProperty(dataType.indexName, "true");
    }

    conf.setProperty(WORKFLOW_ENCODED_DONOR_IDS_PROPERTY,
        job.encodedDonorIds);

    conf.setProperty(WORKFLOW_USER_EMAIL_PROPERTY, job.userEmailAddress);
    conf.setProperty(WORKFLOW_SUPPORT_EMAIL_PROPERTY, supportEmailAddress);

    conf.setProperty(WORKFLOW_DOWNLOAD_ID_PROPERTY, job.downloadId);
    conf.setProperty(WORKFLOW_OUTPUT_DIR_PROPERTY, job.outputDir);
    conf.setProperty(WORKFLOW_STATUS_URL_PROPERTY, job.statusUrl);
    conf.setProperty(WORKFLOW_RELEASE_NAME_PROPERTY, job.releaseName);
    conf.setProperty(OozieClient.APP_PATH, this.appPath);
    // submit and start the workflow job
    return cli.run(conf); // async
  }

  @Builder
  @Data
  public static class ArchiveJobContext {

    private final String userEmailAddress;
    private final String encodedDonorIds;
    private final Map<String, String> jobInfo;
    private final String downloadId;
    private final List<DataType> dataTypes;
    private final String outputDir;
    private final String statusUrl;
    private final String releaseName;
    private final List<SelectionEntry<DataType, String>> filterTypeInfo;
  }

  /**
   * @param oozieId
   * @return
   * @throws OozieClientException
   */
  public Status getStatus(String oozieId) throws OozieClientException {
    OozieClient cli = new OozieClient(oozieUrl);
    WorkflowJob jobInfo = cli.getJobInfo(oozieId);
    return jobInfo.getStatus();
  }

  /**
   * @return
   */
  public String getOozieUrl() {
    return oozieUrl;
  }

}