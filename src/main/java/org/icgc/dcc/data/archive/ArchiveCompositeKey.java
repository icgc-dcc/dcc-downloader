package org.icgc.dcc.data.archive;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class ArchiveCompositeKey {

  int donorId;
  long lineNum;
}