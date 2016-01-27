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
  public static final String DEFAULT_SUPPORT_EMAIL_ADDRESS = "nobody@example.com";
  public static final String DEFAULT_APP_PATH =
      "hdfs://localhost:8020/user/downloader/workflows/archive-main";

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

  public Status getStatus(String oozieId) throws OozieClientException {
    OozieClient cli = new OozieClient(oozieUrl);
    WorkflowJob jobInfo = cli.getJobInfo(oozieId);
    return jobInfo.getStatus();
  }

  public String getOozieUrl() {
    return oozieUrl;
  }

}