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

import lombok.ToString;

import com.beust.jcommander.Parameter;

/**
 * Command line options.
 * 
 */
@ToString
public class Options {

  @Parameter(names = { "-m", "--mount" }, required = false, description = "Mount Point for the download directory")
  public String mount = "/icgc/download";

  @Parameter(names = { "-b", "--buffersize" }, required = false, description = "buffer size for download")
  public int bufferSize = 32768;

  @Parameter(names = { "-p", "--port" }, required = false, description = "SSH Port")
  public int port = 5323;

  @Parameter(names = { "-r", "--release" }, required = false, description = "Release to extract, load and transform")
  public String release;

  @Parameter(names = { "-v", "--version" }, help = true, description = "Show version information")
  public boolean version;

  @Parameter(names = { "-ln", "--currentReleaseSymlink" }, required = true, help = true, description = "current release symlink")
  public String symlink;

  @Parameter(names = { "-h", "--help" }, help = true, description = "Show help information")
  public boolean help;

}
