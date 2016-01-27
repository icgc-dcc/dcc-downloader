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

import static org.icgc.dcc.downloader.core.ArchiverConstant.END_OF_LINE;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import lombok.Data;
import lombok.Builder;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.ArchiveMetaManager;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.DataType;
import org.icgc.dcc.downloader.core.SchemaUtil;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

@Builder
@Data
public class ArchiverTestingUtility {

  private static final String DELIMITER = Bytes.toString(ArchiverConstant.TSV_DELIMITER);
  private HTable metaTable;
  private HTable dataTable;
  private HTable archiveTable;
  private DataType dataType;
  private ArchiveMetaManager metaManager;
  private long totalLine;
  private int numberOfDonors;
  private int numberOfColumns;

  public Map<Integer, String> pupolateTestData() throws IOException {
    ImmutableMap.Builder<Integer, String> b = ImmutableMap.builder();
    for (int donorId = 1; donorId <= numberOfDonors; ++donorId) {
      b.put(donorId, simulateExporter(donorId));
    }
    return b.build();
  }

  private String simulateExporter(Integer donorId) throws IOException {
    // Prepare metaTable with all the attributes for the data type
    StringBuilder sb = new StringBuilder();
    String[] headers = new String[numberOfColumns];
    for (int n = 0; n < numberOfColumns; ++n) {
      headers[n] = "H-" + RandomStringUtils.randomAlphabetic(3);
    }
    Random ran = new Random();

    long totalSize = 0;
    for (long i = 1; i <= totalLine; ++i) {
      byte[] rowKey = SchemaUtil.encodedArchiveRowKey(donorId, i);
      Put line = new Put(rowKey);
      for (byte n = 0; n < numberOfColumns; ++n) {
        String value = "";
        if (n == 0 || ran.nextFloat() > 0.5) {
          value = RandomStringUtils.randomAlphabetic(5);
          byte[] bytes = Bytes.toBytes(value);
          totalSize = totalSize + bytes.length;
          line.add(ArchiverConstant.DATA_CONTENT_FAMILY, new byte[] { n }, bytes);
        }
        sb.append(value);
        sb.append(DELIMITER);
      }
      dataTable.put(line);
      sb.setLength(sb.length() - DELIMITER.length());
      sb.append(Bytes.toString(END_OF_LINE));
    }
    metaManager.keepMetaInfo(dataType.indexName, donorId, totalLine, totalSize, headers);
    return sb.toString();
  }

  public void close() throws IOException {
    metaManager.close();
    dataTable.flushCommits();
  }

  public Set<Integer> getDonorIds() {
    ImmutableSortedSet.Builder<Integer> idBuilder = ImmutableSortedSet.naturalOrder();
    for (int i = 1; i <= numberOfDonors; ++i) {
      idBuilder.add(i);
    }
    return idBuilder.build();
  }
}
