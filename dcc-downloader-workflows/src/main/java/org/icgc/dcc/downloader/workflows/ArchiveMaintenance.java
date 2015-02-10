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

import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.icgc.dcc.downloader.core.DataType;
import org.icgc.dcc.downloader.core.SchemaUtil;

/**
 * Cleans out dynamic download archives
 */
@Slf4j
public class ArchiveMaintenance {

	private final Configuration conf;
	private final String releaseName;

	public ArchiveMaintenance(String releaseName, Configuration conf) {

		this.conf = conf;
		this.releaseName = releaseName;
	}

	public void run() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			for (DataType dataType : DataType.values()) {
				String tableName = SchemaUtil.getDataTableName(
						dataType.indexName, releaseName);
				log.info("Table maintenannce for: {}", tableName);
				try {
				admin.majorCompact(tableName);
				do {
					TimeUnit.MINUTES.sleep(2);
				} while (admin.getCompactionState(tableName) != CompactionState.NONE);
				} catch (TableNotFoundException e) {
					log.warn("Skipping table {}", tableName);
				}

			}
		} catch (Exception e) {
			log.error("Failed to maintenance", e);
			throw e;
		} finally {
			admin.close();
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String releaseName = args[0];
		conf.set("hbase.zookeeper.quorum", args[1]);
		ArchiveMaintenance maintenance = new ArchiveMaintenance(releaseName,
				conf);
		maintenance.run();
	}
}
