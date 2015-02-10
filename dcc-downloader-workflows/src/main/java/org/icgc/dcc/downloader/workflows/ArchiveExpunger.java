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
package org.icgc.dcc.downloader.workflows;

import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_END_TIME_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TTL_COLUMN;

import java.io.IOException;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.icgc.dcc.downloader.core.ArchiveJobManager;
import org.icgc.dcc.downloader.core.TableResourceManager;

import com.google.common.collect.ImmutableSet;

/**
 * Cleans out dynamic download archives
 */
@Slf4j
public class ArchiveExpunger {

  private final Path dynamicDownloadPath;
  private final Configuration conf;
  private final ArchiveJobManager archiveManager;

  public ArchiveExpunger(Path dynamicDownloadPath, Configuration conf, ArchiveJobManager mgr) {
    this.dynamicDownloadPath = dynamicDownloadPath;
    this.conf = conf;
    this.archiveManager = mgr;
  }

  public void run() throws Exception {
    log.info("Running archive expunger");
    long currentTime = System.currentTimeMillis();

    try {
      FileSystem fs = FileSystem.get(conf);
      RemoteIterator<LocatedFileStatus> files = fs.listLocatedStatus(dynamicDownloadPath);
      while (files.hasNext()) {
        LocatedFileStatus file = files.next();
        String downloadIdStr = file.getPath().getName();

        Map<String, Map<String, String>> infoMap = archiveManager.getJobInfo(ImmutableSet.of(downloadIdStr));
        Map<String, String> jobInfo = infoMap.get(downloadIdStr);

        log.info("Checking " + downloadIdStr);

        if (null != jobInfo && null != jobInfo.get(ARCHIVE_END_TIME_COLUMN)) {
          long endTime = Long.parseLong(jobInfo.get(ARCHIVE_END_TIME_COLUMN));
          long ttl = Long.parseLong(jobInfo.get(ARCHIVE_TTL_COLUMN)) * 60L * 60L * 1000L;

          if (endTime + ttl < currentTime) {
            log.info("Should be deleted");
            try {
              if (false == fs.delete(file.getPath(), true)) {
                log.warn("Failed to delete " + file.getPath().toString());
              }
            } catch (IOException e) {
              log.error("Error deleting " + file.getPath().toString());
              throw e;
            }
          }
        } else {
          log.warn("Cannot determine job state, skipping");
        }
      }
    } catch (Exception e) {
      log.error("Failed to run archive expunger", e);
      throw e;
    }
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    Path downloadDir = new Path(args[0]);
    conf.set("hbase.zookeeper.quorum", args[1]);

    // Only need the getJobInfo, getJobStatus.
    ArchiveJobManager mgr = new ArchiveJobManager(new TableResourceManager(conf), null, (byte) 0);
    ArchiveExpunger archiveExpunger = new ArchiveExpunger(downloadDir, conf, mgr);
    archiveExpunger.run();
  }
}
