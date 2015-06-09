package org.icgc.dcc.downloader.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class RenameTable {

  private static void printUsage() {
    System.out.println("Usage: " + RenameTable.class.getCanonicalName()
        + " <OLD_NAME> <NEW_NAME>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      System.exit(1);
    }
    String oldTableName = args[0];
    String newTableName = args[1];
    String tempTableName = oldTableName;

    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    do {
      tempTableName = "_tmp_" + tempTableName;
    } while (admin.isTableAvailable(tempTableName));

    admin.disableTable(oldTableName);
    admin.snapshot(tempTableName, oldTableName);
    admin.cloneSnapshot(tempTableName, newTableName);
    admin.deleteSnapshot(tempTableName);
    admin.deleteTable(oldTableName);
    admin.close();
  }
}
