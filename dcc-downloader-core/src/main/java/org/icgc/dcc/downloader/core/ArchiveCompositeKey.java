package org.icgc.dcc.downloader.core;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class ArchiveCompositeKey {

	int donorId;
	long lineNum;
}