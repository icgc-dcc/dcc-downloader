%default JSON_LOADER 'com.twitter.elephantbird.pig.load.JsonLoader';
%default OBSERVATION '/icgc/etl/r-53049_0-965_0_all/mirna.json/part-*';

DEFINE ExtractId org.icgc.dcc.piggybank.ExtractId();

-- load observation
observation = LOAD '$OBSERVATION' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as document:map[];

-- filter and denormalize for mirna 
mirna = FILTER observation BY document#'_type' == 'mirna_seq';
selected_mirna = foreach mirna generate 
                                        ExtractId(document#'_donor_id') as donor_id:int,
					document#'_donor_id' as icgc_donor_id,
                                        document#'_project_id' as project_code,
                                        document#'_specimen_id' as icgc_specimen_id,
                                        document#'_sample_id' as icgc_sample_id,
				        document#'analyzed_sample_id' as submitted_sample_id, 
                                        document#'analysis_id' as analysis_id,
                                        document#'mirna_db' as mirna_db,
                                        document#'mirna_id' as mirna_id,
                                        document#'normalized_read_count' as normalized_read_count,
                                        document#'raw_read_count' as raw_read_count,
                                        document#'fold_change' as fold_change,
                                        document#'is_isomir' as is_isomir,
                                        document#'chromosome' as chromosome,
                                        document#'chromosome_start' as chromosome_start,
                                        document#'chromosome_end' as chromosome_end,
                                        document#'chromosome_strand' as chromosome_strand,
                                        document#'assembly_version' as assembly_version,
                                        document#'verification_status' as verification_status,
                                        document#'verification_platform' as verification_platform,
                                        document#'sequencing_platform' as sequencing_platform,
                                        document#'total_read_count' as total_read_count,
                                        document#'experimental_protocol' as experimental_protocol,
                                        document#'reference_sample_type' as reference_sample_type,
                                        document#'alignment_algorithm' as alignment_algorithm,
                                        document#'normalization_algorithm' as normalization_algorithm,
                                        document#'other_analysis_algorithm' as other_analysis_algorithm,
                                        document#'sequencing_strategy' as sequencing_strategy,
                                        document#'raw_data_repository' as raw_data_repository,
                                        document#'raw_data_accession' as raw_data_accession;