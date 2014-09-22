package org.icgc.dcc.data.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FileInfo {
  
  private final String name;
  private final String type;
  private final long size;
  private final long date;
}
