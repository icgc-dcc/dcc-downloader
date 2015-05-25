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

import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_ACTIVE_GRACE_PERIOD;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_ACTIVE_JOB_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_BLOCK_SIZE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_CURRENT_RELEASE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_FILE_SIZE_COLUMN;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_JOB_INFO_CLIENT_COLUMN_PREFIX;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_JOB_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_JOB_INFO_SYSTEM_COLUMN_PREFIX;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_STATS_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TABLE_NAME;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_BLOCK_SIZE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_CONTENT_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DATA_TYPE_SEPARATOR;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DONOR_ID_LINE_IN_BYTES;
import static org.icgc.dcc.downloader.core.ArchiverConstant.DONOR_ID_SIZE_IN_BYTES;
import static org.icgc.dcc.downloader.core.ArchiverConstant.HEADER_SEPARATOR;
import static org.icgc.dcc.downloader.core.ArchiverConstant.ICGC_DONOR_ID_PREFIX;
import static org.icgc.dcc.downloader.core.ArchiverConstant.MAX_DATA_FILE_SIZE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_BLOCK_SIZE;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_SIZE_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TABLE_NAME;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TYPE_INFO_FAMILY;
import static org.icgc.dcc.downloader.core.ArchiverConstant.POSTFIX_ALL;
import static org.icgc.dcc.downloader.core.ArchiverConstant.TABLENAME_SEPARATOR;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.icgc.dcc.downloader.core.ArchiveJobManager.JobProgress;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

@Slf4j
public final class SchemaUtil {

	private static final byte[][] SPLIT_KEYS = new byte[][] { { '0' }, { '1' },
			{ '2' }, { '3' }, { '4' }, { '5' }, { '6' }, { '7' }, { '8' },
			{ '9' }, { 'a' }, { 'b' }, { 'c' }, { 'd' }, { 'e' }, { 'f' } };

	private SchemaUtil() {
		throw new RuntimeException(
				"please don't try to instantiate a utility class");
	}

	public static void createDataTable(String tablename) throws IOException {
		createDataTable(tablename, ImmutableList.<byte[]> of(),
				HBaseConfiguration.create());

	}

	public static void createDataTable(String tablename, Configuration conf)
			throws IOException {
		createDataTable(tablename, ImmutableList.<byte[]> of(), conf, true);

	}

	public static void createDataTable(String tablename, Configuration conf,
			boolean withSnappyCompression) throws IOException {
		createDataTable(tablename, ImmutableList.<byte[]> of(), conf,
				withSnappyCompression);
	}

	public static void createDataTable(String tablename,
			List<byte[]> boundaries, Configuration conf) throws IOException {
		createDataTable(tablename, boundaries, conf, true);
	}

	public static void createDataTable(String tablename,
			List<byte[]> boundaries, Configuration conf,
			boolean withSnappyCompression) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			if (!admin.tableExists(tablename)) {
				byte[][] splits = new byte[boundaries.size()][];
				admin.createTable(
						getDataTableSchema(tablename, withSnappyCompression),
						boundaries.toArray(splits));
			}
		} catch (TableExistsException e) {
			log.warn("already created... (skip)", e);
		} finally {
			admin.close();
		}
	}

	public static HTableDescriptor getDataTableSchema(String tablename) {
		return getDataTableSchema(tablename, true);
	}

	public static HTableDescriptor getDataTableSchema(String tablename,
			boolean withSnappyCompression) {
		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tablename));
		HColumnDescriptor dataSchema = new HColumnDescriptor(
				DATA_CONTENT_FAMILY);
		dataSchema.setBlockCacheEnabled(false);
		dataSchema.setBlocksize(DATA_BLOCK_SIZE);
		dataSchema.setBloomFilterType(BloomType.ROW);
		dataSchema.setDataBlockEncoding(DataBlockEncoding.PREFIX);
		if (withSnappyCompression)
			dataSchema.setCompressionType(Algorithm.SNAPPY);
		dataSchema.setMaxVersions(1);
		descriptor.addFamily(dataSchema);
		descriptor.setMaxFileSize(MAX_DATA_FILE_SIZE);
		return descriptor;
	}

	public static int extractId(String donorId) {
		Preconditions.checkState(donorId.startsWith(ICGC_DONOR_ID_PREFIX));
		return Integer.valueOf(StringUtils.substringAfter(donorId.trim(),
				ICGC_DONOR_ID_PREFIX));
	}

	public static byte[] encodedDonorId(String donorId) {
		return Bytes.toBytes(extractId(donorId));
	}

	public static String decodeDonorId(byte[] donorId) {
		return decodeDonorId(Bytes.toInt(donorId));
	}

	public static String decodeDonorId(int donorId) {
		return ICGC_DONOR_ID_PREFIX + donorId;
	}

	public static void createMetaTable(String tablename) throws IOException {
		createMetaTable(tablename, HBaseConfiguration.create(), true);
	}

	public static void deleteTables(String regex) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTableDescriptor[] descs = admin.listTables(regex);
			if (descs != null) {
				for (val desc : descs) {
					if (admin.isTableEnabled(desc.getName())) {
						admin.disableTable(desc.getName());
					}
					admin.deleteTable(desc.getName());
				}
			} else {
				System.out.println("Table does not exist. Not deleted. : "
						+ regex);
			}
		} finally {
			admin.close();
		}
	}

	public static void createMetaTable(String tablename, Configuration conf,
			boolean withSnappyCompression) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			if (!admin.tableExists(tablename)) {
				HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tablename));
				HColumnDescriptor dataTypeSchema = new HColumnDescriptor(
						META_TYPE_INFO_FAMILY);
				dataTypeSchema.setBlockCacheEnabled(true);
				dataTypeSchema.setInMemory(true);
				dataTypeSchema.setBlocksize(META_BLOCK_SIZE);
				dataTypeSchema.setBloomFilterType(BloomType.ROWCOL);
				if (withSnappyCompression)
					dataTypeSchema.setCompressionType(Algorithm.SNAPPY);
				dataTypeSchema.setMaxVersions(1);
				descriptor.addFamily(dataTypeSchema);

				HColumnDescriptor dataSizeSchema = new HColumnDescriptor(
						META_SIZE_INFO_FAMILY);
				dataSizeSchema.setBlockCacheEnabled(true);
				dataSizeSchema.setInMemory(true);
				dataSizeSchema.setBlocksize(META_BLOCK_SIZE);
				dataSizeSchema.setBloomFilterType(BloomType.ROWCOL);
				if (withSnappyCompression)
					dataSizeSchema.setCompressionType(Algorithm.SNAPPY);
				dataSizeSchema.setMaxVersions(1);
				descriptor.addFamily(dataSizeSchema);
				
				admin.createTable(descriptor, metaSplitKeys());
			}
		} catch (TableExistsException e) {
			log.warn("It has been created.", e);
		} finally {
			admin.close();
		}
	}

	private static byte[][] metaSplitKeys() {
		byte[][] splitKeys = new byte[DataType.values().length][];
		int i =0;
		for(DataType type : DataType.values()) {
			splitKeys[i] = Bytes.toBytes(type.indexName);
			++i;
		}
		Arrays.sort(splitKeys, new Bytes.ByteArrayComparator());
		return Arrays.copyOfRange(splitKeys, 1, splitKeys.length);
	}

	public static String getMetaTableName(String releaseName) {
		if (releaseName.equals("")
				|| releaseName.equals(ARCHIVE_CURRENT_RELEASE)) {
			return META_TABLE_NAME;
		} else {
			return META_TABLE_NAME + TABLENAME_SEPARATOR + releaseName;
		}
	}

	public static String getDataTableName(String dataType, String releaseName) {
		if (releaseName.equals("")
				|| releaseName.equals(ARCHIVE_CURRENT_RELEASE)) {
			return dataType;
		} else {
			return dataType + TABLENAME_SEPARATOR + releaseName;
		}
	}

	public static void createArchiveTable() throws IOException {
		createArchiveTable(HBaseConfiguration.create(), true);
	}

	public static void checkTableIntegrity(String tableName) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin.checkHBaseAvailable(conf);

		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			if (!admin.isTableAvailable(tableName)) {
				throw new RuntimeException("Table does not available: "
						+ tableName);
			}
			executeHBCK(tableName, conf);
		} finally {
			admin.close();
		}
	}

	private static void executeHBCK(String tableName, Configuration conf)
			throws IOException, ClassNotFoundException {
		Path hbasedir = new Path(conf.get(HConstants.HBASE_DIR));
		URI defaultFs = hbasedir.getFileSystem(conf).getUri();
		conf.set("fs.defaultFS", defaultFs.toString()); // for hadoop 0.21+
		conf.set("fs.default.name", defaultFs.toString()); // for hadoop 0.20

		int numThreads = conf.getInt("hbasefsck.numthreads", 50);
		ExecutorService exec = new ScheduledThreadPoolExecutor(numThreads);
		HBaseFsck hbck = new HBaseFsck(conf, exec);
		int retcode = hbck.getRetCode();
		if (retcode != 0)
			throw new RuntimeException("Please check the table for problems: "
					+ tableName);
	}

	public static void majorCompact(String tablename) throws IOException,
			InterruptedException {
		majorCompact(tablename, HBaseConfiguration.create());
	}

	public static boolean isTableExists(String tablename) throws IOException,
			InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			return admin.isTableAvailable(tablename);
		} finally {
			admin.close();
		}
	}

	public static void majorCompact(String tablename, Configuration conf)
			throws IOException, InterruptedException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			admin.majorCompact(tablename);
		} finally {
			admin.close();
		}
	}

	public static void createArchiveTable(Configuration conf)
			throws IOException {
		createArchiveTable(conf, true);
	}

	public static void createArchiveTable(Configuration conf,
			boolean withCompression) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			if (!admin.tableExists(ARCHIVE_TABLE_NAME)) {
				HTableDescriptor descriptor = getArchiveHTableDescriptor(withCompression);
				admin.createTable(descriptor, getSplitKeyForArchiveTable());
			}
		} catch (TableExistsException e) {
			log.warn("already created... (skip)", e);
		} finally {
			admin.close();
		}
	}

	public static HTableDescriptor getArchiveHTableDescriptor(
			boolean withCompression) {
		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(ARCHIVE_TABLE_NAME));
		HColumnDescriptor activeJobSchema = new HColumnDescriptor(
				ARCHIVE_ACTIVE_JOB_FAMILY);
		activeJobSchema.setBlockCacheEnabled(false);
		activeJobSchema.setInMemory(false);
		activeJobSchema.setTimeToLive(ARCHIVE_ACTIVE_GRACE_PERIOD);
		activeJobSchema.setBlocksize(ARCHIVE_BLOCK_SIZE);
		activeJobSchema.setBloomFilterType(BloomType.ROWCOL);
		if (withCompression)
			activeJobSchema.setCompressionType(Algorithm.SNAPPY);
		activeJobSchema.setMaxVersions(1);
		descriptor.addFamily(activeJobSchema);

		HColumnDescriptor statsSchema = new HColumnDescriptor(
				ARCHIVE_STATS_INFO_FAMILY);
		statsSchema.setBlockCacheEnabled(true);
		statsSchema.setInMemory(true);
		statsSchema.setBlocksize(ARCHIVE_BLOCK_SIZE);
		statsSchema.setBloomFilterType(BloomType.ROWCOL);
		if (withCompression)
			statsSchema.setCompressionType(Algorithm.SNAPPY);
		statsSchema.setMaxVersions(1);
		descriptor.addFamily(statsSchema);

		HColumnDescriptor jobSchema = new HColumnDescriptor(
				ARCHIVE_JOB_INFO_FAMILY);
		jobSchema.setBlockCacheEnabled(true);
		jobSchema.setInMemory(true);
		jobSchema.setBlocksize(ARCHIVE_BLOCK_SIZE);
		jobSchema.setBloomFilterType(BloomType.ROWCOL);
		if (withCompression)
			jobSchema.setCompressionType(Algorithm.SNAPPY);
		jobSchema.setMaxVersions(1);
		descriptor.addFamily(jobSchema);

		return descriptor;
	}

	private static byte[][] getSplitKeyForArchiveTable() {
		return SPLIT_KEYS;

	}

	public static byte[] encodedArchiveRowKey(int donorId, long lineNumber) {
		return Bytes.add(Bytes.toBytes(donorId), Bytes.toBytes(lineNumber));
	}

	public static ArchiveCompositeKey decodeArchiveRowKey(byte[] encodedRowKey) {
		int donorId = Bytes.toInt(encodedRowKey, 0);
		long line = Bytes.toLong(encodedRowKey, 4);
		return new ArchiveCompositeKey(donorId, line);
	}

	public static byte[] encodeHeader(String[] headers) {
		return Bytes.toBytes(StringUtils.join(headers, HEADER_SEPARATOR));
	}

	public static String[] decodeHeader(byte[] encodedheader) {
		return StringUtils.split(Bytes.toString(encodedheader),
				HEADER_SEPARATOR);
	}

	public enum JobInfoType {
		SYSTEM, CLIENT;
	}

	public static ImmutableMap<JobInfoType, ImmutableMap<String, String>> decodeJobInfo(
			Map<byte[], byte[]> jobInfoMap) {
		ImmutableMap.Builder<String, String> clientInfoMapBuilder = ImmutableMap
				.builder();
		ImmutableMap.Builder<String, String> systemInfoMapBuilder = ImmutableMap
				.builder();
		for (val entry : jobInfoMap.entrySet()) {
			byte[] encodedKey = entry.getKey();
			byte[] value = entry.getValue();
			if (Bytes.startsWith(encodedKey,
					ARCHIVE_JOB_INFO_SYSTEM_COLUMN_PREFIX)) {
				byte[] key = Bytes.tail(encodedKey, encodedKey.length
						- ARCHIVE_JOB_INFO_SYSTEM_COLUMN_PREFIX.length);
				systemInfoMapBuilder.put(Bytes.toString(key),
						Bytes.toString(value));

			} else {
				byte[] key = Bytes.tail(encodedKey, encodedKey.length
						- ARCHIVE_JOB_INFO_CLIENT_COLUMN_PREFIX.length);

				if (Bytes.equals(key, Bytes.toBytes(ARCHIVE_FILE_SIZE_COLUMN))) {
					clientInfoMapBuilder.put(Bytes.toString(key),
							String.valueOf(Bytes.toLong(value)));
				} else {
					clientInfoMapBuilder.put(Bytes.toString(key),
							Bytes.toString(value));

				}

			}
		}
		return ImmutableMap.of(JobInfoType.CLIENT,
				clientInfoMapBuilder.build(), JobInfoType.SYSTEM,
				systemInfoMapBuilder.build());

	}

	public static ImmutableMap<byte[], byte[]> encodeClientJobInfo(
			Map<String, String> jobInfoMap) {
		ImmutableMap.Builder<byte[], byte[]> infoMapBuilder = ImmutableMap
				.builder();
		for (val entry : jobInfoMap.entrySet()) {
			byte[] encodedKey = Bytes.add(
					ArchiverConstant.ARCHIVE_JOB_INFO_CLIENT_COLUMN_PREFIX,
					Bytes.toBytes(entry.getKey()));
			byte[] value = Bytes.toBytes(entry.getValue());
			infoMapBuilder.put(encodedKey, value);
		}
		return infoMapBuilder.build();
	}

	public static byte[] encodeSystemJobInfoKey(String systemKey) {
		return Bytes.add(
				ArchiverConstant.ARCHIVE_JOB_INFO_SYSTEM_COLUMN_PREFIX,
				Bytes.toBytes(systemKey));
	}

	public static Map<byte[], byte[]> encodeSystemJobInfo(
			Map<String, String> jobInfoMap) {
		ImmutableMap.Builder<byte[], byte[]> infoMapBuilder = ImmutableMap
				.builder();
		for (val entry : jobInfoMap.entrySet()) {
			byte[] encodedKey = encodeSystemJobInfoKey(entry.getKey());
			byte[] value = Bytes.toBytes(entry.getValue());
			infoMapBuilder.put(encodedKey, value);
		}
		return infoMapBuilder.build();
	}

	public static String encodeTypeInfo(
			List<SelectionEntry<DataType, String>> filterTypeInfo) {
		StringBuilder sb = new StringBuilder();

		if (filterTypeInfo.size() == 0) {
			return "";
		}
		for (val selection : filterTypeInfo) {
			sb.append(selection.getKey().indexName);
			sb.append(DATA_TYPE_SEPARATOR);
		}
		sb.setLength(sb.length() - DATA_TYPE_SEPARATOR.length());
		return sb.toString();
	}

	public static Map<byte[], byte[]> encodeStatsInfo(
			Map<DataType, JobProgress> statsInfoMap) {

		ImmutableMap.Builder<byte[], byte[]> infoMapBuilder = ImmutableMap
				.builder();
		for (val entry : statsInfoMap.entrySet()) {
			infoMapBuilder.put(Bytes.add(
					Bytes.toBytes(entry.getKey().indexName), POSTFIX_ALL),
					Bytes.toBytes(entry.getValue().getDenominator()));
			infoMapBuilder.put(Bytes.toBytes(entry.getKey().indexName),
					Bytes.toBytes(entry.getValue().getNumerator()));
		}
		return infoMapBuilder.build();
	}

	public static Map<DataType, JobProgress> decodeStatsInfo(
			Map<byte[], byte[]> statsInfoMap) {

		HashMap<DataType, JobProgress> progressMap = Maps
				.newHashMapWithExpectedSize(statsInfoMap.size() / 2);
		for (val entry : statsInfoMap.entrySet()) {
			byte[] coln = entry.getKey();
			long size = Bytes.toLong(entry.getValue());
			if (Bytes.equals(coln, coln.length - 1, 1, POSTFIX_ALL, 0,
					POSTFIX_ALL.length)) {
				// this is the total size
				DataType dataType = DataType.valueOf(Bytes.toString(coln, 0,
						coln.length - 1).toUpperCase());

				if (progressMap.containsKey(dataType)) {
					progressMap.get(dataType).setDenominator(size);
				} else {
					progressMap.put(dataType, new JobProgress(0, size));
				}
			} else {
				DataType dataType = DataType.valueOf(Bytes.toString(coln)
						.toUpperCase());
				if (progressMap.containsKey(dataType)) {
					progressMap.get(dataType).setNumerator(size);
				} else {
					progressMap.put(dataType, new JobProgress(size, 0));
				}
			}
		}
		return progressMap;
	}

	public static byte[] encodeSizeInfo(SizeInfo sizeInfo) {
		return Bytes.add(Bytes.toBytes(sizeInfo.getTotalLine()),
				Bytes.toBytes(sizeInfo.getTotalSize()));
	}

	public static SizeInfo decodeSizeInfo(byte[] sizeInfo) {
		long totalLine = Bytes.toLong(sizeInfo, 0, DONOR_ID_LINE_IN_BYTES);
		long totalSize = Bytes.toLong(sizeInfo, DONOR_ID_LINE_IN_BYTES,
				DONOR_ID_SIZE_IN_BYTES);
		return new SizeInfo(totalLine, totalSize);
	}
}
