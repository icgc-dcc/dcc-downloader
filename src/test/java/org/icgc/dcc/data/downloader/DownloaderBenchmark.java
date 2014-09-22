/**
 * Copyright 2013(c) The Ontario Institute for Cancer Research. All rights reserved.
 * 
 * This program and the accompanying materials are made available under the terms of the GNU Public
 * License v3.0. You should have received a copy of the GNU General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.data.downloader;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import lombok.SneakyThrows;
import lombok.val;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.icgc.dcc.data.downloader.DynamicDownloader.DataType;

import com.google.caliper.Benchmark;
import com.google.caliper.model.ArbitraryMeasurement;
import com.google.caliper.runner.CaliperMain;
import com.google.common.collect.Lists;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Sets;

/**
 * 
 */
public class DownloaderBenchmark {

  public static class DynamicDownloaderBenchmark extends Benchmark {

    private FileSystem fs;

    private Path downloadPath;

    private DynamicDownloader downloader;

    private ArrayList<String> donors;

    private ArrayList<Text> keys;

    @SneakyThrows
    @Override
    protected void setUp() {
      String downloadDir = "hdfs://***REMOVED***:8020/icgc/download/dynamic/dev";
      this.downloadPath = new Path(downloadDir);
      downloader =
          new DynamicDownloader(downloadDir, "localhost", ArchiveJobSubmitter.DEFAULT_OOZIE_URL,
              ArchiveJobSubmitter.DEFAULT_APP_PATH,
              ArchiveJobSubmitter.DEFAULT_SUPPORT_EMAIL_ADDRESS);
      Configuration conf = new Configuration();
      fs = FileSystem.get(downloadPath.toUri(), conf);

      // TODO: Use Reader(Configuration, Option...) instead
      Reader idxReader = new SequenceFile.Reader(conf, Reader.file(new Path(downloadPath, "index")));
      Text key = new Text();
      donors = Lists.newArrayListWithCapacity(50000);
      keys = Lists.newArrayListWithCapacity(5000);
      while (idxReader.next(key)) {
        keys.add(key);
        String[] parts = key.toString().split(Pattern.quote("."));
        donors.add(parts[1]);
      }
      System.out.println("Total number of donors: " + donors.size());

      Collections.shuffle(donors);
    }

    @SneakyThrows
    @ArbitraryMeasurement(units = "Bytes/second", description = "Transfer Rate")
    public double downloadAll() {
      CountingOutputStream out =
          new CountingOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/data-all.tar.gz")));
      // CountingOutputStream out = new CountingOutputStream(new NullOutputStream());
      List<SelectionEntry<DataType, String>> selections = Lists.newArrayList();
      selections.add(new SelectionEntry<DataType, String>(DataType.METH_SEQ, "TSV"));
      // selections.add(new SelectionEntry("SSM", "TSV"));
      HashSet<String> donor100 = Sets.newHashSet(donors);
      Map<DataType, Future<Long>> sizeMap = downloader.getSizes(donor100);
      long total = 0;
      for (val entry : sizeMap.entrySet()) {
        total = total + entry.getValue().get();
      }

      long start = System.nanoTime();
      // downloader.streamArchive(out, donor100, selections);
      long diff = System.nanoTime() - start;

      System.out.println("Actual Data Size Transfer Rate: " + total / (diff / 1000000000.0));
      return out.getByteCount() / (diff / 1000000000.0);
    }

    @SneakyThrows
    @ArbitraryMeasurement(units = "Bytes/second", description = "Transfer Rate")
    public double download10Percent() {
      // CountingOutputStream out = new CountingOutputStream(new NullOutputStream());
      CountingOutputStream out =
          new CountingOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/data-old.tar.gz")));
      List<SelectionEntry<DataType, String>> selections = Lists.newArrayList();
      selections.add(new SelectionEntry<DataType, String>(DataType.SSM_OPEN, "TSV"));

      long start = System.nanoTime();
      // downloader.streamArchive(out, donor10, selections);
      long diff = System.nanoTime() - start;

      return out.getByteCount() / (diff / 1000000000.0);
    }

    @SneakyThrows
    @ArbitraryMeasurement(units = "Bytes/second", description = "Transfer Rate")
    public double mapNext() {
      MapFile.Reader reader = new MapFile.Reader(downloadPath, fs.getConf());
      CountingOutputStream out = new CountingOutputStream(new NullOutputStream());
      Text key = new Text();
      StreamingOutputBytesWritable value = new StreamingOutputBytesWritable(out);

      long start = System.nanoTime();
      while (reader.next(key, value)) {
      }
      long diff = System.nanoTime() - start;

      return out.getByteCount() / (diff / 1000000000.0);
    }

    @SneakyThrows
    @ArbitraryMeasurement(units = "Bytes/second", description = "Transfer Rate")
    public double mapGetClosest() {
      MapFile.Reader reader = new MapFile.Reader(downloadPath, fs.getConf());
      CountingOutputStream out = new CountingOutputStream(new NullOutputStream());

      Text key = new Text();
      Text nextKey = new Text("!");

      StreamingOutputBytesWritable value = new StreamingOutputBytesWritable(out);
      long start = System.nanoTime();
      while ((key = (Text) reader.getClosest(nextKey, value, false)) != null) {
        nextKey = new Text(key + "!");
      }
      long diff = System.nanoTime() - start;

      return out.getByteCount() / (diff / 1000000000.0);
    }

    @SneakyThrows
    @ArbitraryMeasurement(units = "Bytes/second", description = "Transfer Rate")
    public double mapSeek() {
      MapFile.Reader reader = new MapFile.Reader(downloadPath, fs.getConf());
      CountingOutputStream out = new CountingOutputStream(new NullOutputStream());

      StreamingOutputBytesWritable value = new StreamingOutputBytesWritable(out);
      long start = System.nanoTime();
      for (Text key : keys) {
        reader.seek(key);
        reader.get(key, value);
      }
      long diff = System.nanoTime() - start;

      return out.getByteCount() / (diff / 1000000000.0);
    }

  }

  public static void main(String[] args) {
    CaliperMain.main(DynamicDownloaderBenchmark.class, args);
  }
}
