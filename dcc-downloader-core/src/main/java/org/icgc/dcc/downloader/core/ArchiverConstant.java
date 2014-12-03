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

final public class ArchiverConstant {

  public static final String TABLENAME_SEPARATOR = ".";
  public static final byte[] TSV_DELIMITER = new byte[] { '\t' };
  public static final long MAX_TAR_ENTRY_SIZE_IN_BYTES = 3221225472L;
  public static final byte[] END_OF_LINE = new byte[] { 10 }; // LF character

  // DATA Table
  public static final byte[] DATA_CONTENT_FAMILY = new byte[] { 'd' };
  public static final int DATA_BLOCK_SIZE = 5242880;
  public static final int MAX_DATA_FILE_SIZE = 524288000;

  // ARCHIVE Table
  public static final String ARCHIVE_TABLE_NAME = "archive";

  public static final String ARCHIVE_CURRENT_RELEASE = "CURRENT";
  public static final byte[] ARCHIVE_SYSTEM_KEY = new byte[] { '.', 'M', 'E', 'T', 'A', '.' };
  public static final byte[] ARCHIVE_DOWNLOAD_COUNTER_COLUMN = new byte[] { 't' };

  public static final byte[] ARCHIVE_STATS_INFO_FAMILY = new byte[] { 's' };
  public static final byte[] ARCHIVE_JOB_INFO_FAMILY = new byte[] { 'j' };
  public static final byte[] ARCHIVE_JOB_INFO_SYSTEM_COLUMN_PREFIX = new byte[] {
      's', ':' };
  public static final byte[] ARCHIVE_JOB_INFO_CLIENT_COLUMN_PREFIX = new byte[] {
      'c', ':' };
  public static final byte[] ARCHIVE_ACTIVE_JOB_FAMILY = new byte[] { 'a' };
  public static final byte[] ARCHIVE_ACTIVE_DOWNLOAD_COUNTER_COLUMN = new byte[] { 'd' };

  public static final int ARCHIVE_ACTIVE_GRACE_PERIOD = 86400; // 1 day
  public static final int ARCHIVE_BLOCK_SIZE = 65536;

  public static final String ARCHIVE_USER_EMAIL_COLUMN = "m";
  public static final String ARCHIVE_ENCODED_DONOR_IDS_COLUMN = "d";
  public static final String ARCHIVE_DATA_TYPE_INFO_COLUMN = "t";
  public static final String ARCHIVE_TTL_COLUMN = "ttl";
  public static final String ARCHIVE_END_TIME_COLUMN = "et";
  public static final String ARCHIVE_FILE_SIZE_COLUMN = "fileSize";
  public static final String ARCHIVE_OOZIE_ID_COLUMN = "oid";

  public static final String DATA_TYPE_SEPARATOR = ",";

  // META Table
  public static final String META_TABLE_NAME = "meta";
  public static final byte[] META_TYPE_INFO_FAMILY = new byte[] { 't' };
  public static final byte[] META_TYPE_HEADER = new byte[] { 'h' };
  public static final String HEADER_SEPARATOR = ",";
  public static final int META_BLOCK_SIZE = 65536;
  public static final byte[] META_SIZE_INFO_FAMILY = new byte[] { 's' };

  public static final int DONOR_ID_SIZE_IN_BYTES = 8;
  public static final int DONOR_ID_LINE_IN_BYTES = 8;

  public static final byte[] POSTFIX_ALL = new byte[] { '*' };

  public static final String ICGC_DONOR_ID_PREFIX = "DO";

  // Oozie Workflow
  public static final String WORKFLOW_DATA_TYPE_PROPERTY = "dataType";
  public static final String WORKFLOW_USER_NAME_PROPERTY = "user.name";
  public static final String WORKFLOW_USER_EMAIL_PROPERTY = "userEmailAddress";
  public static final String WORKFLOW_SUPPORT_EMAIL_PROPERTY = "supportEmailAddress";
  public static final String WORKFLOW_ENCODED_DONOR_IDS_PROPERTY = "encodedDonorIds";
  public static final String WORKFLOW_DOWNLOAD_ID_PROPERTY = "downloadId";
  public static final String WORKFLOW_OUTPUT_DIR_PROPERTY = "outputDir";
  public static final String WORKFLOW_STATUS_URL_PROPERTY = "statusUrl";
  public static final String WORKFLOW_RELEASE_NAME_PROPERTY = "releaseName";

}
