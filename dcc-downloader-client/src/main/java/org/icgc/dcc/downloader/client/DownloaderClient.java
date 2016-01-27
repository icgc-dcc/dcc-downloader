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
package org.icgc.dcc.downloader.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.icgc.dcc.downloader.core.ArchiveJobManager;
import org.icgc.dcc.downloader.core.ArchiveJobSubmitter;
import org.icgc.dcc.downloader.core.DataType;
import org.icgc.dcc.downloader.core.DonorIdEncodingUtil;
import org.icgc.dcc.downloader.core.SelectionEntry;
import org.icgc.dcc.downloader.core.TableResourceManager;
import org.icgc.dcc.downloader.core.ArchiveJobManager.JobStatus;
import org.icgc.dcc.downloader.core.ArchiveJobSubmitter.ArchiveJobContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class DownloaderClient {

  private static final byte DEFAULT_THRESHOLD = 20;

  private static final String DEFAULT_RELEASE_NAME = "";

  private final Path dynamicDownloadPath;

  private final Configuration conf;

  private final ArchiveJobSubmitter jobSubmitter;

  private final ArchiveOutputStream streamer;

  private Map<DataType, Map<String, Long>> sizeInfoMap;

  private final ArchiveJobManager archiveManager;

  public DownloaderClient(String downloadRootDir) {
    this(downloadRootDir, "localhost",
        ArchiveJobSubmitter.DEFAULT_OOZIE_URL,
        ArchiveJobSubmitter.DEFAULT_APP_PATH,
        ArchiveJobSubmitter.DEFAULT_SUPPORT_EMAIL_ADDRESS);
  }

  public DownloaderClient(String downloadRootDir, String quorum, String oozieUrl, String appPath,
      String supportEmailAddress) {

    this(downloadRootDir, quorum, oozieUrl, appPath, supportEmailAddress, DEFAULT_THRESHOLD, DEFAULT_RELEASE_NAME);

  }

  public DownloaderClient(String downloadRootDir, String quorum, String oozieUrl, String appPath,
      String supportEmailAddress, byte capacityThreshold, String releaseName) {

    this(downloadRootDir, createConf(quorum), oozieUrl, appPath, supportEmailAddress, capacityThreshold, releaseName);

  }

  public DownloaderClient(String downloadRootDir, Configuration conf, String oozieUrl, String appPath,
      String supportEmailAddress) {
    this(downloadRootDir, conf, new ArchiveJobSubmitter(oozieUrl, appPath, supportEmailAddress),
        new ArchiveJobManager(new TableResourceManager(conf), oozieUrl, DEFAULT_THRESHOLD));
  }

  public DownloaderClient(String downloadRootDir, Configuration conf, String oozieUrl, String appPath,
      String supportEmailAddress, byte threshold) {
    this(downloadRootDir, conf, new ArchiveJobSubmitter(oozieUrl, appPath, supportEmailAddress),
        new ArchiveJobManager(new TableResourceManager(conf), oozieUrl, threshold));
  }

  public DownloaderClient(String downloadRootDir, Configuration conf, String oozieUrl, String appPath,
      String supportEmailAddress, byte threshold, String releaseName) {
    this(downloadRootDir, conf, new ArchiveJobSubmitter(oozieUrl, appPath, supportEmailAddress),
        new ArchiveJobManager(new TableResourceManager(conf, releaseName), oozieUrl,
            threshold));
  }

  @SneakyThrows
  public DownloaderClient(String downloadRootDir, Configuration conf,
      ArchiveJobSubmitter jobSubmitter, ArchiveJobManager archiveManager) {
    log.info("Dynamic Download URL: {}", downloadRootDir);
    this.dynamicDownloadPath = new Path(downloadRootDir);
    this.conf = conf;
    this.jobSubmitter = jobSubmitter;
    this.archiveManager = archiveManager;
    this.streamer = new ArchiveOutputStream(this.dynamicDownloadPath, this.conf);
    this.sizeInfoMap = archiveManager.getSizeInfo();
    log.debug("Cache size: {}", sizeInfoMap);

  }

  private static Configuration createConf(String quorum) {
    Configuration conf = HBaseConfiguration.create(new Configuration());
    conf.set("hbase.zookeeper.quorum", quorum);
    return conf;
  }

  // can be easily cacheable because it is an estimate anyway
  public Map<DataType, Future<Long>> getSizes(final Set<String> donorIds) {
    ImmutableMap.Builder<DataType, Future<Long>> resultMapBuilder = ImmutableMap.builder();
    ExecutorService exec = Executors.newFixedThreadPool(DataType.values().length);
    try {
      for (val sizeEntry : sizeInfoMap.entrySet()) {
        Future<Long> f = exec.submit(new Callable<Long>() {

          @Override
          public Long call() throws Exception {
            long total = 0;
            Map<String, Long> donorSizeMap = sizeEntry.getValue();
            for (val donorId : donorIds) {
              if (donorSizeMap.containsKey(donorId)) {
                total = total + donorSizeMap.get(donorId);
              }
            }
            return total;
          }
        });
        resultMapBuilder.put(sizeEntry.getKey(), f);
      }
    } finally {
      exec.shutdown();
    }
    return resultMapBuilder.build();
  }

  public String submitJob(Set<String> donorIds,
      final List<SelectionEntry<DataType, String>> filterTypeInfo,
      final Map<String, String> jobInfo, String statusUrl) throws IOException {

    return submitJob(donorIds,
        filterTypeInfo,
        jobInfo, jobSubmitter.getSupportEmailAddress(),
        statusUrl);

  }

  public String submitJob(Set<String> donorIds,
      final List<SelectionEntry<DataType, String>> filterTypeInfo,
      final Map<String, String> jobInfo, final String userEmailAddress,
      String downloadUrl) throws IOException {
    final String encodedDonorIds = DonorIdEncodingUtil.encodeDonorIds(donorIds);
    try {
      ImmutableList.Builder<DataType> typeBuilder = ImmutableList.builder();
      for (val info : filterTypeInfo) {
        typeBuilder.add(info.getKey());
      }
      final List<DataType> dataTypes = typeBuilder.build();

      final String downloadId = archiveManager.retrieveDownloadId();
      final String releaseName = archiveManager.getReleaseName();
      Path outputPath = new Path(this.dynamicDownloadPath, downloadId);

      // job context
      ArchiveJobContext jobContext = ArchiveJobContext.builder()
          .dataTypes(dataTypes).downloadId(downloadId)
          .encodedDonorIds(encodedDonorIds)
          .outputDir(outputPath.toString())
          .filterTypeInfo(filterTypeInfo)
          .jobInfo(jobInfo)
          .releaseName(releaseName)
          .userEmailAddress(userEmailAddress).statusUrl(downloadUrl + "/" + downloadId)
          .build();

      // submit the workflow
      final String oozieId = jobSubmitter.submit(jobContext);
      // record the jobContext for future reference
      archiveManager.recordJobInfo(jobContext, oozieId);
      return downloadId;

    } catch (Exception e) {
      log.error("Fail to submit job", e);
      throw new RuntimeException(e);
    }
  }

  public Map<String, Map<String, String>> getJobInfo(Set<String> downloadIds) {
    return archiveManager.getJobInfo(downloadIds);
  }

  public boolean isServiceAvailable() {
    return archiveManager.isServiceAvailable();
  }

  public boolean isOverCapacity() {
    return archiveManager.isOverCapacity();
  }

  public boolean streamArchiveInGz(OutputStream out, String downloadId,
      DataType dataType) throws IOException {

    Map<DataType, List<String>> lookup = archiveManager.getHeader(ImmutableList.of(dataType));
    return streamer.streamArchiveInGz(out, downloadId, dataType, lookup);
  }

  public boolean streamArchiveInGzTar(OutputStream out, String downloadId,
      List<DataType> downloadDataTypes) throws IOException {

    Map<DataType, List<String>> lookup = archiveManager.getHeader(downloadDataTypes);
    // TODO: start record active job
    try {
      archiveManager.flagActiveJob(downloadId);
      return streamer.streamArchiveInGzTar(out, downloadId, downloadDataTypes, lookup);
    } finally {
      // TODO: end record active job
      archiveManager.flagInactiveJob(downloadId);
    }
  }

  public Map<String, JobStatus> getStatus(Set<String> downloadIds) throws IOException {
    return archiveManager.getStatus(downloadIds);
  }

  public JobStatus cancelJob(String downloadId) throws IOException {
    return archiveManager.cancel(downloadId);
  }
}