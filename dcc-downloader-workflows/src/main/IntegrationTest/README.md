Dynamic Downloader Integration Test 
===

This is a set of tools for testing dynamic downloader with the portal api. 
It queries a running portal to get a filter condition that is based on a set of donor ids. The filter condition is subsequently used to request the portal to generate an archive. 

The Integration Test will wait until the archive is generated and then run a diff between the static download files and the generated archive filtered by the filter condition.


Run
---
To run the test:

HADOOP_USER_NAME=downloader pig -Dpig.additional.jars=jyson-1.0.2.jar runner.py  -t DATA_TYPE -p PROJECT_CODES -n TEST_RUN_LABEL -i HDFS_ROOT_DIR_FOR_STATIC_DOWNLOAD

- DATA_TYPE: the data type under test which can be ssm_open, mirna_seq, etc
- PROJECT_CODES: this is a comma separated lists of project codes
- TEST_RUN_LABEL: this is the name used to identify the test run 
- HDFS_ROOT_DIR_FOR_STATIC_DOWNLOAD: this is the root directory in HDFS where the archive for static download is found.
