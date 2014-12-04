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
package org.icgc.dcc.downloader.core;

import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hbase.util.Base64;
import org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection;
import org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection.Builder;

import com.google.protobuf.InvalidProtocolBufferException;

@Slf4j
public final class DonorIdEncodingUtil {

  public static final int BASE64_OPTIONS = Base64.GZIP | Base64.DONT_BREAK_LINES | Base64.URL_SAFE;

  public static List<Integer> convertCodeToList(byte[] encodedDonorIds) {
    try {
      return DonorSelection.parseFrom(encodedDonorIds).getIdList();
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to decode donor Ids: {}", encodedDonorIds, e);
      throw new RuntimeException(e);
    }
  }

  public static List<Integer> convertCodeToList(String encodedDonorIds) {
    try {
      return DonorSelection.parseFrom(Base64.decode(encodedDonorIds, BASE64_OPTIONS)).getIdList();
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to decode donor Ids: {}", encodedDonorIds, e);
      throw new RuntimeException(e);
    }
  }

  public static byte[] convertCodeToBytes(String encodedDonorIds) {
    return Base64.decode(encodedDonorIds, BASE64_OPTIONS);
  }

  public static String encodeDonorIds(Set<String> donorIds) {
    Builder builder = DonorSelection.newBuilder();
    for (String donorId : donorIds) {
      builder.addId(SchemaUtil.extractId(donorId));
    }
    return Base64.encodeBytes(builder.build().toByteArray(), BASE64_OPTIONS);
  }

  public static String encodeIds(Set<Integer> donorIds) {
    DonorSelection selection = DonorSelection.newBuilder().addAllId(donorIds).build();
    return Base64.encodeBytes(selection.toByteArray(), BASE64_OPTIONS);
  }
}
