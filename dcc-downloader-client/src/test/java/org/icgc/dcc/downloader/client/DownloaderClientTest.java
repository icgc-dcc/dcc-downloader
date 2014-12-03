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
package org.icgc.dcc.downloader.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.oozie.client.OozieClientException;
import org.icgc.dcc.downloader.client.DownloaderClient;
import org.icgc.dcc.downloader.core.ArchiveJobManager;
import org.icgc.dcc.downloader.core.ArchiveJobSubmitter;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.DataType;
import org.icgc.dcc.downloader.core.DonorIdEncodingUtil;
import org.icgc.dcc.downloader.core.SchemaUtil;
import org.icgc.dcc.downloader.core.SelectionEntry;
import org.icgc.dcc.downloader.core.SizeInfo;
import org.icgc.dcc.downloader.core.TableResourceManager;
import org.icgc.dcc.downloader.core.ArchiveJobSubmitter.ArchiveJobContext;
import org.icgc.dcc.downloader.workflows.ArchiverTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class DownloaderClientTest {

  private static HBaseTestingUtility utility;
  private static Configuration conf;
  private static TableResourceManager manager;
  private static ArchiveMetaManager metaManager;
  private static ArchiverTestingUtility TU;
  private static HTable archiveTable;
  private static HTable dataTable;
  private static HTable metaTable;
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
    TU.pupolateTestData();
    donorIds = TU.getDonorIds();
    TU.close();

  }

  @AfterClass
  public static void cleanupCluster() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Test
  public void testGetSizeWithValidDonorId() throws InterruptedException, ExecutionException, IOException {
    DownloaderClient dl =
        new DownloaderClient("/tmp/download/dynamic", conf, null, null, "***REMOVED***");
    for (Integer donorId : donorIds) {
      long actualSize =
          dl.getSizes(ImmutableSet.<String> of(SchemaUtil.decodeDonorId(donorId))).get(DataType.SSM_OPEN).get();
      Get sizeInfo = new Get(Bytes.toBytes(DataType.SSM_OPEN.indexName));
      sizeInfo.addColumn(ArchiverConstant.META_SIZE_INFO_FAMILY, Bytes.toBytes(donorId));
      Result result = metaTable.get(sizeInfo);
      assertFalse(result.isEmpty());
      SizeInfo sizeMap =
          SchemaUtil.decodeSizeInfo(result.getColumnLatest(ArchiverConstant.META_SIZE_INFO_FAMILY,
              Bytes.toBytes(donorId))
              .getValue());
      assertEquals(sizeMap.getTotalSize(), actualSize);
    }
  }

  @Test
  public void testGetSizeWithNoInfoType() throws InterruptedException, ExecutionException, IOException {
    DownloaderClient dl =
        new DownloaderClient("/tmp/download/dynamic", conf, null, null, "***REMOVED***");
    for (Integer donorId : donorIds) {
      long actualSize =
          dl.getSizes(ImmutableSet.<String> of(SchemaUtil.decodeDonorId(donorId))).get(DataType.CNSM).get();
      assertEquals(0, actualSize);
    }
  }

  @Test
  public void testGetSizeWithInvalidDonorId() throws InterruptedException, ExecutionException, IOException {
    DownloaderClient dl =
        new DownloaderClient("/tmp/download/dynamic", conf, null, null, "***REMOVED***");
    long actualSize =
        dl.getSizes(ImmutableSet.<String> of("DOES_NOT_EXIST")).get(DataType.SSM_OPEN).get();
    assertEquals(0, actualSize);
  }

  @After
  public void tearDown() throws IOException {
    utility.getHBaseAdmin().disableTable(archiveTable.getTableName());
    utility.getHBaseAdmin().deleteTable(archiveTable.getTableName());

    SchemaUtil.createArchiveTable(conf, false);
  }

  @Test
  public void testSubmitJob() throws IOException, OozieClientException {
    String downloadTestId = "DOWNLOAD_ID";
    String oozieId = "OOZIE_ID";
    String expectedRootOutputDir = "/tmp/download/dynamic";
    String expectedUrl = "TEST_URL";
    String expectedEmailAddress = "***REMOVED***";
    Set<String> expectedDonorIds = ImmutableSet.of("DO1");
    ArchiveJobSubmitter submitter = mock(ArchiveJobSubmitter.class);
    when(submitter.submit(any(ArchiveJobContext.class))).thenReturn(oozieId);

    ArchiveJobManager archiveManager = mock(ArchiveJobManager.class);
    when(archiveManager.retrieveDownloadId()).thenReturn(downloadTestId);

    List<SelectionEntry<DataType, String>> expectedFilterTypeInfo =
        ImmutableList.<SelectionEntry<DataType, String>> of(
            new SelectionEntry<DataType, String>(DataType.SSM_OPEN, "TSV"),
            new SelectionEntry<DataType, String>(DataType.CLINICAL, "TSV")
            );
    ImmutableMap<String, String> expectedJobInfo = ImmutableMap.<String, String> of("KEY", "VALUE");

    DownloaderClient dl =
        new DownloaderClient(expectedRootOutputDir, conf, submitter, archiveManager);
    String expectedDownloadId =
        dl.submitJob(
            expectedDonorIds,
            expectedFilterTypeInfo,
            expectedJobInfo,
            expectedEmailAddress,
            expectedUrl
            );

    ArgumentCaptor<ArchiveJobContext> argument = ArgumentCaptor.forClass(ArchiveJobContext.class);
    verify(submitter).submit(argument.capture());
    verify(archiveManager).recordJobInfo(any(ArchiveJobContext.class), eq(oozieId));

    ArchiveJobContext actualJobContext = argument.getValue();

    assertThat(downloadTestId).isEqualTo(expectedDownloadId);
    assertThat(actualJobContext.getDownloadId()).isEqualTo(expectedDownloadId);
    assertThat(actualJobContext.getDataTypes()).containsOnly(DataType.SSM_OPEN, DataType.CLINICAL);

    assertThat(actualJobContext.getFilterTypeInfo()).containsAll(expectedFilterTypeInfo);
    assertThat(actualJobContext.getJobInfo()).isEqualTo(expectedJobInfo);
    assertThat(actualJobContext.getOutputDir()).isEqualTo(expectedRootOutputDir + "/" + downloadTestId);
    assertThat(actualJobContext.getStatusUrl()).isEqualTo(expectedUrl + "/" + downloadTestId);
    assertThat(actualJobContext.getEncodedDonorIds()).isEqualTo(DonorIdEncodingUtil.encodeDonorIds(expectedDonorIds));
    assertThat(actualJobContext.getUserEmailAddress()).isEqualTo(expectedEmailAddress);

  }
}
