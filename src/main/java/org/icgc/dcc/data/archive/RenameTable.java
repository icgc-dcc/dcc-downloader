package org.icgc.dcc.data.archive;

import java.io.FileNotFoundException;
import java.io.IOException;

import lombok.extern.log4j.Log4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.Writables;

/**
 * This class is for temporary usage. HBase 0.94.x has a solution for that.
 * (https://hbase.apache.org/book/table.rename.html)
 */
@Log4j
public class RenameTable {

  private static void printUsage() {
    System.out.println("Usage: " + RenameTable.class.getCanonicalName() + " <OLD_NAME> <NEW_NAME>");
  }

  private static boolean isDirExists(FileSystem fs, Path dir) throws IOException {
    return (fs.exists(dir) || fs.isDirectory(dir));
  }

  static boolean isTableRegion(byte[] tableName, HRegionInfo hri) {
    return Bytes.equals(Bytes.toBytes(hri.getTableNameAsString()), tableName);
  }

  static HRegionInfo createHRI(byte[] tableName, HRegionInfo oldHRI) {
    return new HRegionInfo(tableName, oldHRI.getStartKey(), oldHRI.getEndKey(), oldHRI.isSplit());
  }

  static HTableDescriptor getHTableDescriptor(byte[] tableName, FileSystem fs, Configuration conf)
      throws FileNotFoundException,
      IOException {
    return FSTableDescriptors.getTableDescriptor(fs, new Path(conf.get(HConstants.HBASE_DIR)), tableName);
  }

  public static void exec(String oldTableName, String newTableName) throws Exception {

    /*
     * Set hadoop filesystem configuration using the hbase.rootdir. Otherwise, we'll always use localhost though the
     * hbase.rootdir might be pointing at hdfs location.
     */
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);

    Path rootdir = FSUtils.getRootDir(conf);
    Path oldTableDir = fs.makeQualified(new Path(rootdir, new Path(oldTableName)));
    if (!isDirExists(fs, oldTableDir)) {
      log.info("Nothing to do. Table does not exist : " + oldTableName);
      return;
    }

    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.flush(oldTableName);
      admin.disableTable(oldTableName);

      Path newTableDir = fs.makeQualified(new Path(rootdir, newTableName));
      if (!fs.exists(newTableDir)) {
        fs.mkdirs(newTableDir);
      } else {
        throw new RuntimeException("Table already exists aborted. (" + newTableName + ")");
      }

      /* Get hold of oldHTableDescriptor and create a new one */
      HTableDescriptor oldHTableDescriptor = getHTableDescriptor(Bytes.toBytes(oldTableName), fs,
          conf);
      HTableDescriptor newHTableDescriptor = new HTableDescriptor(Bytes.toBytes(newTableName));
      for (HColumnDescriptor family : oldHTableDescriptor.getColumnFamilies()) {
        newHTableDescriptor.addFamily(family);
      }

      /*
       * Run through the meta table moving region mentions from old to new table name.
       */
      HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
      try {
        Scan scan = new Scan();
        ResultScanner scanner = metaTable.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
          String rowId = Bytes.toString(result.getRow());
          HRegionInfo oldHRI = Writables.getHRegionInfo(result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
          if (oldHRI == null) {
            throw new IOException("HRegionInfo is null for " + rowId);
          }
          if (!isTableRegion(Bytes.toBytes(oldTableName), oldHRI)) {
            continue;
          }
          log.debug(oldHRI.toString());
          Path oldRDir = new Path(oldTableDir, new Path(oldHRI.getEncodedName()));
          if (!fs.exists(oldRDir)) {
            log.warn(oldRDir.toString() + " does not exist -- region " + oldHRI.getRegionNameAsString());
          }

          /* Now make a new HRegionInfo to add to .META. for the new region. */
          HRegionInfo newHRI = createHRI(Bytes.toBytes(newTableName), oldHRI);
          log.debug("New Region Info: " + newHRI);
          Path newRDir = new Path(newTableDir, new Path(newHRI.getEncodedName()));
          log.debug("Renaming " + oldRDir.toString() + " as " + newRDir.toString());
          // move oldRDir to newRDir
          fs.rename(oldRDir, newRDir);

          /* Removing old region from meta */
          log.info("Removing " + rowId + " from .META.");
          Delete d = new Delete(result.getRow());
          metaTable.delete(d);

          /* Create 'new' region */
          HRegion newR = new HRegion(newTableDir, null, fs, conf, newHRI, newHTableDescriptor, null);
          log.debug("Adding to meta: " + newR.toString());
          byte[] newRbytes = Writables.getBytes(newR.getRegionInfo());
          Put p = new Put(newR.getRegionName());
          p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, newRbytes);
          metaTable.put(p);
          /*
           * Finally update the .regioninfo under new region location so it has new name.
           */
          Path regioninfofile = new Path(newR.getRegionDir(), HRegion.REGIONINFO_FILE);
          fs.delete(regioninfofile, true);
          FSDataOutputStream out = fs.create(regioninfofile);
          newR.getRegionInfo().write(out);
          out.close();
        }
        scanner.close();
        FSTableDescriptors.createTableDescriptor(newHTableDescriptor, conf);
        fs.delete(oldTableDir, true);
      } finally {
        metaTable.close();
      }

      if (admin.isTableEnabled(newTableName)) admin.disableTable(newTableName);
      // we want to re-enable a table
      if (admin.isTableDisabled(newTableName)) admin.enableTable(newTableName);
      while (!admin.isTableAvailable(newTableName)) {
        HBaseFsck.main(new String[] { "-repair", newTableName });
        if (admin.isTableDisabled(newTableName)) admin.enableTable(newTableName);
        log.info("Waiting ...");
        Thread.sleep(5000);
      }
    } finally {
      admin.close();
    }
    log.info("Renaming Completed from: " + oldTableName + " to: " + newTableName);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      System.exit(1);
    }
    String oldTableName = args[0];
    String newTableName = args[1];
    exec(oldTableName, newTableName);
  }
}
