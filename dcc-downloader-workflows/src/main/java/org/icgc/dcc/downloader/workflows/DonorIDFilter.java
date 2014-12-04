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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.TreeSet;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.downloader.core.ArchiverConstant;
import org.icgc.dcc.downloader.core.DonorIdEncodingUtil;

/**
 * A domain specific filter for filtering donor id that is encoded in a byte array
 */
@Slf4j
public class DonorIDFilter extends FilterBase {

  private final static int NON_EXISTENT_ID = -1;
  private final static int DONOR_ID_LENGTH = 4; // byte length

  private boolean filterOutRow = false;
  private boolean isDone = false;
  private byte[] encodedIds;
  private int currentID = NON_EXISTENT_ID;

  private transient NavigableSet<Integer> donorIds;

  public DonorIDFilter() {
    super();
  }

  // it is used in the mapreduce job
  public DonorIDFilter(String encodedIds) {
    this.encodedIds = DonorIdEncodingUtil.convertCodeToBytes(encodedIds);
    initializeDonorId();
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    // filtering out this row meaning all rows associated with the current donor id
    if (this.filterOutRow) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    // include all columns that is associated with this rowkey (donor id + #line)
    return ReturnCode.INCLUDE;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    int id = getNextId();

    log.debug("Next id hint: {}", id);
    byte[] prefix = null;
    if (id == NON_EXISTENT_ID) {
      prefix = HConstants.EMPTY_END_ROW;
      this.isDone = true;
    } else {
      this.currentID = id;
      prefix = Bytes.toBytes(id);
    }

    return KeyValue.createFirstOnRow(prefix, ArchiverConstant.DATA_CONTENT_FAMILY, new byte[0]);
  }

  private int getNextId() {
    // search starts from this.currentID + 1
    donorIds = donorIds.tailSet(this.currentID, false);
    if (donorIds.isEmpty()) {
      return NON_EXISTENT_ID;
    } else {
      return donorIds.first();
    }

  }

  @Override
  public boolean filterAllRemaining() {
    return this.isDone;
  }

  @Override
  public boolean filterRowKey(byte[] data, int offset, int length) {
    int id = Bytes.toInt(Arrays.copyOfRange(data, offset, offset + DONOR_ID_LENGTH));
    log.debug("Row key to be filtered: {}", id);
    if (this.currentID != id) {
      // this is a new donor id
      this.currentID = id;
      // donorIds is not initialize
      if (donorIds != null) {
        this.filterOutRow = !(donorIds.contains(this.currentID));
      } else {
        // include everything
        this.filterOutRow = false;
      }
    }
    return this.filterOutRow;
  }

  @Override
  public boolean filterRow() {
    return this.filterOutRow;
  }

  @Override
  public void reset() {
    this.filterOutRow = false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.encodedIds = Bytes.readByteArray(in);
    initializeDonorId();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.encodedIds);
  }

  private void initializeDonorId() {
    if (encodedIds != null) {
      donorIds = new TreeSet<Integer>(DonorIdEncodingUtil.convertCodeToList(encodedIds));
    }
  }

}
