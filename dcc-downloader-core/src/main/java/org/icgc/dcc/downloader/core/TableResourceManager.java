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

import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TABLE_NAME;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TABLE_NAME;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hbase.client.HTable;

@Slf4j
public class TableResourceManager {

  private final Configuration conf;
  private final String archiveTableName;
  private final String metaTableName;
  private final String releaseName;

  public TableResourceManager(Configuration conf) {
    this.conf = conf;
    this.archiveTableName = ARCHIVE_TABLE_NAME;
    this.metaTableName = META_TABLE_NAME;
    this.releaseName = "";

  }

  public TableResourceManager(Configuration conf, String releaseName) {
    this.conf = conf;
    this.archiveTableName = ARCHIVE_TABLE_NAME;
    this.releaseName = releaseName;
    this.metaTableName = SchemaUtil.getMetaTableName(releaseName);
  }

  public String getReleaseName() {
    return this.releaseName;
  }

  public HTable getTable(String tablename) {
    HTable table = null;
    try {
      table = new HTable(this.conf, tablename);
    } catch (IOException e) {
      log.error("Unable to connect to table: {}", tablename, e);
      throw new RuntimeException(e);
    }
    return table;
  }

  public void closeTable(HTable table) {
    try {
      table.flushCommits();
      table.close();
    } catch (IOException e) {
      log.error("Fail to close table: {}", table, e);
      throw new RuntimeException(e);
    }
  }

  public HTable getArchiveTable() {
    return getTable(archiveTableName);
  }

  public HTable getMetaTable() {
    return getTable(metaTableName);
  }

  public byte getFreeDiskSpacePercentage() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FsStatus status = fs.getStatus();
    return (byte) (((double) status.getRemaining() / status.getCapacity()) * 100);
  }
}
