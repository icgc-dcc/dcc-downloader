package org.icgc.dcc.downloader.core;

public enum DataType {

  // Type name is used system-wide
  // display name is used for portal
  SSM_OPEN("ssm", "ssm_open", false), CLINICALSAMPLE("clinicalsample",
      "clinicalsample", false), CLINICAL("clinical", "clinical", false),

  DONOR("donor", "donor", false), DONOR_FAMILY("donor_family",
      "donor_family", false), DONOR_THERAPY("donor_therapy",
      "donor_therapy", false), DONOR_EXPOSURE("donor_exposure",
      "donor_exposure", false), SPECIMEN("specimen", "specimen", false), SAMPLE(
      "sample", "sample", false),

  CNSM("cnsm", "cnsm", false), JCN("jcn", "jcn", false), METH_SEQ("meth_seq",
      "meth_seq", false), METH_ARRAY("meth_array", "meth_array", false), MIRNA_SEQ(
      "mirna_seq", "mirna_seq", false), STSM("stsm", "stsm", false), PEXP(
      "pexp", "pexp", false), EXP_SEQ("exp_seq", "exp_seq", false), EXP_ARRAY(
      "exp_array", "exp_array", false), SSM_CONTROLLED("ssm",
      "ssm_controlled", true), SGV_CONTROLLED("sgv", "sgv_controlled",
      true),

  // for backward compatible only (remove when no longer use these names)
  EXP("exp", "exp", false), MIRNA("mirna", "mirna", false), METH("meth",
      "meth", false);

  public final String name;

  public final String indexName;

  public final boolean isControlled;

  private DataType(String name, String indexName, boolean isControlled) {
    this.name = name;
    this.indexName = indexName;
    this.isControlled = isControlled;
  }
}