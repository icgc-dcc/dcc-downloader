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

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lombok.val;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.DataType;
import org.icgc.dcc.downloader.core.DonorIdEncodingUtil;
import org.icgc.dcc.downloader.core.SchemaUtil;
import org.icgc.dcc.downloader.core.TableResourceManager;
import org.icgc.dcc.downloader.workflows.Archiver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

public class ArchiverTest {

  private static HBaseTestingUtility utility;
  private static Configuration conf;
  private static TableResourceManager manager;
  private static ArchiveMetaManager metaManager;
  private static ArchiverTestingUtility TU;
  private static HTable archiveTable;
  private static HTable dataTable;
  private static HTable metaTable;
  private static Map<Integer, String> content;
  private static Set<Integer> donorIds;

  @BeforeClass
  public static void setupCluster() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
    conf = utility.getHBaseCluster().getConfiguration();
    SchemaUtil.createArchiveTable(conf, false);
    SchemaUtil.createMetaTable(ArchiverConstant.META_TABLE_NAME, conf, false);
    SchemaUtil.createDataTable(DataType.SSM_OPEN.indexName, conf, false);
    manager = new TableResourceManager(conf);
    metaManager = new ArchiveMetaManager(conf);
    archiveTable = manager.getTable(ArchiverConstant.ARCHIVE_TABLE_NAME);
    dataTable = manager.getTable(DataType.SSM_OPEN.indexName);
    metaTable = manager.getTable(ArchiverConstant.META_TABLE_NAME);
    TU = ArchiverTestingUtility.builder()
        .archiveTable(archiveTable)
        .metaManager(metaManager)
        .dataTable(dataTable)
        .dataType(DataType.SSM_OPEN)
        .numberOfColumns(40)
        .totalLine(100)
        .numberOfDonors(2)
        .build();
    content = TU.pupolateTestData();
    donorIds = TU.getDonorIds();
    TU.close();
  }

  @AfterClass
  public static void cleanupCluster() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Test
  public void sanity() throws Exception {
    assertTrue(utility.getHBaseAdmin().isTableAvailable(ArchiverConstant.ARCHIVE_TABLE_NAME));
    String expectedContent = prepareExpectedContent(donorIds);
    String[] lines = expectedContent.split(Bytes.toString(ArchiverConstant.END_OF_LINE));
    assertEquals(TU.getNumberOfColumns(), lines[0].split(Bytes.toString(ArchiverConstant.TSV_DELIMITER), -1).length);

    System.out.println("Number of lines: " + lines.length);
    assertEquals(TU.getNumberOfDonors() * TU.getTotalLine(), lines.length);

    Scan scan = new Scan();
    ResultScanner scanner = dataTable.getScanner(scan);
    long actualNumberOfLines = 0;
    for (Result result : scanner) {
      actualNumberOfLines++;
      NavigableMap<byte[], byte[]> map = result.getFamilyMap(ArchiverConstant.DATA_CONTENT_FAMILY);
      assertTrue(TU.getNumberOfColumns() > map.keySet().size());
      for (val key : map.keySet()) {
        assertNotNull(key);
      }
    }

    Result result = metaTable.get(new Get(Bytes.toBytes(DataType.SSM_OPEN.indexName)));
    NavigableMap<byte[], byte[]> sizeMap = result.getFamilyMap(ArchiverConstant.META_SIZE_INFO_FAMILY);
    assertEquals(TU.getNumberOfDonors(), sizeMap.size());
    assertEquals(TU.getNumberOfDonors() * TU.getTotalLine(), actualNumberOfLines);

  }

  @Test
  public void downloadAllData() throws Exception {
    // check if the number of expected column
    // check if the number of expected lines
    test(donorIds);
  }

  @Test
  public void downloadOneDonor() throws Exception {
    // List<Integer> ids = Lists.newArrayList(donorIds);
    // Collections.shuffle(ids);
    for (Integer donorId : donorIds) {
      test(Sets.newHashSet(donorId));
    }
  }

  @Test
  @Ignore
  public void downloadSubsetDonors() throws Exception {
    fail("not implemented");
  }

  private void test(Set<Integer> donorIds) throws Exception {
    String expectedContent = prepareExpectedContent(donorIds);

    String encodedDonorIds = DonorIdEncodingUtil.encodeIds(donorIds);
    String archiveDir = "/tmp/21/ssm_open";
    String[] args =
        new String[] {
            "-D",
            "archive.data.type=ssm_open",
            "-D",
            "encoded.donor.id=" + encodedDonorIds,
            "-D",
            "archive.download.id=test",
            "-D",
            "archive.output.dir=" + archiveDir,
            "-D",
            "archive.time.to.live=" + 48,
            "-D",
            "archive.release.name=CURRENT"
        };
    Archiver.mainForTest(args, conf);
    FileSystem fs = FileSystem.get(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(archiveDir), false);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      GZIPInputStream fis = new GZIPInputStream(fs.open(file.getPath()));
      IOUtils.copy(fis, os);
    }

    assertEquals(expectedContent, os.toString());
  }

  /**
   * @param donorIds2
   * @return
   */
  private String prepareExpectedContent(Set<Integer> donorIds) {
    Iterator<Integer> itr = donorIds.iterator();
    StringBuilder sb = new StringBuilder();
    while (itr.hasNext()) {
      sb.append(content.get(itr.next()));
    }
    return sb.toString();
  }
}
