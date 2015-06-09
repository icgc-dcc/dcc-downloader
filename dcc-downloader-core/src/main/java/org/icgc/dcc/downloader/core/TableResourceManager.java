package org.icgc.dcc.downloader.core;

import static org.icgc.dcc.downloader.core.ArchiverConstant.ARCHIVE_TABLE_NAME;
import static org.icgc.dcc.downloader.core.ArchiverConstant.META_TABLE_NAME;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hbase.client.HTable;

@Slf4j
public class TableResourceManager {

	private final Configuration conf;
	private final String archiveTableName;
	private final String metaTableName;
	private final String releaseName;

	public TableResourceManager(Configuration conf) {
		this.conf = conf;
		this.archiveTableName = ARCHIVE_TABLE_NAME;
		this.metaTableName = META_TABLE_NAME;
		this.releaseName = "";

	}

	public TableResourceManager(Configuration conf, String releaseName) {
		this.conf = conf;
		this.archiveTableName = ARCHIVE_TABLE_NAME;
		this.releaseName = releaseName;
		this.metaTableName = SchemaUtil.getMetaTableName(releaseName);
	}

	public String getReleaseName() {
		return this.releaseName;
	}

	public HTable getTable(String tablename) {
		HTable table = null;
		try {
			table = new HTable(this.conf, tablename);
		} catch (IOException e) {
			log.error("Unable to connect to table: {}", tablename, e);
			throw new RuntimeException(e);
		}
		return table;
	}

	public void closeTable(HTable table) {
		try {
			table.flushCommits();
			table.close();
		} catch (IOException e) {
			log.error("Fail to close table: {}", table, e);
			throw new RuntimeException(e);
		}
	}

	public HTable getArchiveTable() {
		return getTable(archiveTableName);
	}

	public HTable getMetaTable() {
		return getTable(metaTableName);
	}

	public byte getFreeDiskSpacePercentage() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FsStatus status = fs.getStatus();
		return (byte) (((double) status.getRemaining() / status.getCapacity()) * 100);
	}
}
