/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.data.viewer.sftp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.sshd.server.SshFile;
import org.icgc.dcc.data.common.ExportedDataFileSystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

@Slf4j
public class ReadonlyExportedDataSshFile implements SshFile {

  private final File path;

  private final ExportedDataFileSystem fs;

  public ReadonlyExportedDataSshFile(File path, ExportedDataFileSystem fs) {
    this.path = path;
    this.fs = fs;
  }

  @SneakyThrows
  @Override
  public boolean doesExist() {
    try {
      return fs.exists(path);
    } catch (IOException e) {
      log.error("Path is inappropriated: {}", path, e);
      throw e;
    }
  }

  @Override
  public boolean isReadable() {
    return true;
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public boolean isRemovable() {
    return false;
  }

  @SneakyThrows
  @Override
  public long getLastModified() {
    try {
      return this.fs.getModificationTime(path);
    } catch (Exception e) {
      log.error("Path is inappropriated : {}", path, e);
      throw e;
    }
  }

  @Override
  public boolean setLastModified(long time) {
    // not writable
    return false;
  }

  @SneakyThrows
  @Override
  public long getSize() {
    try {
      return this.fs.getSize(path);
    } catch (Exception e) {
      log.error("Path is inappropriated : {}", path, e);
      throw e;
    }

  }

  @Override
  public String getOwner() {
    return "dcc";
  }

  @Override
  public boolean mkdir() {
    return false;
  }

  @Override
  public boolean delete() {
    return false;
  }

  @Override
  public boolean move(SshFile destination) {
    return false;
  }

  @Override
  public OutputStream createOutputStream(long offset) throws IOException {

    throw new IOException("SFTP is in readonly mode");
  }

  @Override
  public InputStream createInputStream(long offset) throws IOException {
    return fs.createInputStream(path, offset);
  }

  @Override
  public void handleClose() throws IOException {
  }

  @Override
  public String getAbsolutePath() {
    return fs.getAbsolutePath(path);
  }

  @Override
  public String getName() {
    return fs.getName(path);
  }

  @SneakyThrows
  @Override
  public boolean isDirectory() {
    try {
      return fs.isDirectory(path);
    } catch (IOException e) {
      log.error("Path is inappropriated : {}", path, e);
      throw e;
    }
  }

  @SneakyThrows
  @Override
  public boolean isFile() {
    try {
      return fs.isFile(path);
    } catch (IOException e) {
      log.error("Path is inappropriated : {}", path, e);
      throw e;
    }
  }

  @Override
  public SshFile getParentFile() {
    return new ReadonlyExportedDataSshFile(path.getParentFile(), fs);
  }

  @Override
  public boolean create() throws IOException {
    return false;
  }

  @Override
  public void truncate() throws IOException {
    throw new IOException("SFTP is in readonly mode");
  }

  @SneakyThrows
  @Override
  public List<SshFile> listSshFiles() {
    Builder<SshFile> b = ImmutableList.<SshFile> builder();
    try {
      for (File file : fs.listFiles(path)) {
        b.add(new ReadonlyExportedDataSshFile(file, fs));
      }
      return b.build();
    } catch (Exception e) {
      log.error("Path is inappropriated : {}", path, e);
      throw e;
    }

  }
}
