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
package org.icgc.dcc.data.archive;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.icgc.dcc.data.archive.ArchiverConstant.DATA_CONTENT_FAMILY;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.fest.assertions.core.Condition;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * 
 */
public class DonorIDFilterTest {

  @Test
  public void testIncludeAll() {

    DonorIDFilter filter = new DonorIDFilter();
    for (int i = 0; i < 1000; ++i) {
      byte[] key = Bytes.toBytes(11);
      assertThat(filter.filterRowKey(key, 0, 4)).isFalse();
      assertThat(filter.filterRow()).isFalse();
      assertThat(filter.filterAllRemaining()).isFalse();
    }
  }

  @Test
  public void testSubset() {

    String encodedDonorIds = DonorIdEncodingUtil.encodeDonorIds(ImmutableSet.<String> of("DO100", "DO13", "DO10"));
    DonorIDFilter filter = new DonorIDFilter(encodedDonorIds);

    // 1
    assertThat(filter.filterRowKey(Bytes.toBytes(1), 0, 4)).isTrue();
    assertThat(filter.filterRow()).isTrue();
    assertThat(filter.filterAllRemaining()).isFalse();

    // any kv
    KeyValue anyKV = mock(KeyValue.class);

    assertThat(filter.getNextKeyHint(anyKV))
        .isNotNull()
        .has(new Condition<KeyValue>() {

          @Override
          public boolean matches(KeyValue kv) {
            int actualId = Bytes.toInt(kv.getRow());
            return (actualId == 10 && Bytes.equals(kv.getFamily(), DATA_CONTENT_FAMILY));
          }
        });

    filter.reset();
    // 10
    assertThat(filter.filterRowKey(Bytes.toBytes(10), 0, 4)).isFalse();
    assertThat(filter.filterRow()).isFalse();
    assertThat(filter.filterAllRemaining()).isFalse();

    filter.reset();
    // 11
    assertThat(filter.filterRowKey(Bytes.toBytes(11), 0, 4)).isTrue();
    assertThat(filter.filterRow()).isTrue();
    assertThat(filter.filterAllRemaining()).isFalse();

    assertThat(filter.getNextKeyHint(anyKV))
        .isNotNull()
        .has(new Condition<KeyValue>() {

          @Override
          public boolean matches(KeyValue kv) {
            int actualId = Bytes.toInt(kv.getRow());
            return (actualId == 13 && Bytes.equals(kv.getFamily(), DATA_CONTENT_FAMILY));
          }
        });

    filter.reset();

    // 13
    assertThat(filter.filterRowKey(Bytes.toBytes(13), 0, 4)).isFalse();
    assertThat(filter.filterRow()).isFalse();
    assertThat(filter.filterAllRemaining()).isFalse();

    filter.reset();

    // 14
    assertThat(filter.filterRowKey(Bytes.toBytes(14), 0, 4)).isTrue();
    assertThat(filter.filterRow()).isTrue();
    assertThat(filter.filterAllRemaining()).isFalse();

    filter.reset();

    assertThat(filter.getNextKeyHint(anyKV))
        .isNotNull()
        .has(new Condition<KeyValue>() {

          @Override
          public boolean matches(KeyValue kv) {
            int actualId = Bytes.toInt(kv.getRow());
            return (actualId == 100 && Bytes.equals(kv.getFamily(), DATA_CONTENT_FAMILY));
          }
        });

    // 100
    assertThat(filter.filterRowKey(Bytes.toBytes(100), 0, 4)).isFalse();
    assertThat(filter.filterRow()).isFalse();
    assertThat(filter.filterAllRemaining()).isFalse();

    filter.reset();

    // 14
    assertThat(filter.filterRowKey(Bytes.toBytes(105), 0, 4)).isTrue();
    assertThat(filter.filterRow()).isTrue();
    assertThat(filter.filterAllRemaining()).isFalse();

    filter.reset();

    assertThat(filter.getNextKeyHint(anyKV))
        .isNotNull()
        .has(new Condition<KeyValue>() {

          @Override
          public boolean matches(KeyValue kv) {
            return (Bytes.equals(kv.getRow(), HConstants.EMPTY_END_ROW) && Bytes.equals(kv.getFamily(),
                DATA_CONTENT_FAMILY));
          }
        });
  }
}
