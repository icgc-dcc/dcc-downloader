ICGC DCC - Data Downloader
===

Data Archive and Download component for DCC 

Build
---

From the command line:

	mvn package

Download Files
---
	
For performance reasons (faster download speed for users), each file contains information for a specific donor and a specific data type.

To concatenate all files of the type pexp for example, one option is to execute the following the command:

    ls *pexp.tsv | head -n 1 | xargs head -n 1 > pexp-all.tsv; find . -name "*pexp.tsv" -exec tail -n +2 {} >> pexp-all.tsv \;