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
package org.icgc.dcc.downloader.core.deprecated;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

@Slf4j
public class Index {

  private static final String idx = "download.index";

  @SneakyThrows
  public static void main(String[] argv) {

    Path downloadPath = new Path(argv[0]);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (fs.exists(downloadPath)) {

      Path idxPath = new Path(downloadPath, idx);
      if (fs.exists(idxPath)) {
        fs.delete(idxPath, false);
      }

      FileStatus[] statuses = fs.listStatus(downloadPath);
      // create out after listing
      @Cleanup
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
          fs.create(idxPath), Charsets.US_ASCII));
      for (FileStatus status : statuses) {
        String filename = status.getPath().getName();
        long filesize = status.getLen();
        out.write(filename + "\t" + filesize);
        out.newLine();
      }
    } else
      log.error("Dynamic download path does not exist: {}", downloadPath);
  }
}
