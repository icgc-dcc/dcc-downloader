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
package org.icgc.dcc.downloader.client;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;

public class StreamingOutputBytesWritable extends BytesWritable {

  private final OutputStream out;

  private boolean skip;

  public StreamingOutputBytesWritable() {
    super();
    out = null;
    skip = false;
  }

  public StreamingOutputBytesWritable(OutputStream out) {
    this.out = out;
  }

  public StreamingOutputBytesWritable(byte[] testdata) {
    super(testdata);
    out = null;
  }

  public StreamingOutputBytesWritable(byte[] bytes, int length) {
    super(bytes, length);
    out = null;
  }

  void setSkip(boolean skip) {
    this.skip = skip;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (out == null) super.readFields(in);
    else {
      if (in instanceof InputStream) {
        // important! Discard the first 4 bytes.
        int numBytes = in.readInt();
        if (skip) {
          // Skip all bytes
          IOUtils.skipFully((InputStream) in, numBytes);
        } else {
          IOUtils.copy((InputStream) in, out);
        }
      } else {
        throw new UnsupportedDataTypeException();
      }
    }
  }
}
