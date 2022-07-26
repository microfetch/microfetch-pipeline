import os
from django.db import models
from enum import Enum

LENGTH_ACCESSION = 40
LENGTH_SHORT = 256
LENGTH_MEDIUM = 1024
LENGTH_LONG = 4096


class AssemblyStatus(Enum):
    SKIPPED = 'skipped'
    WAITING = 'waiting'
    IN_PROGRESS = 'in progress'
    FAIL = 'fail'
    SUCCESS = 'success'


class Taxons(models.Model):
    id = models.PositiveBigIntegerField(primary_key=True)
    last_updated = models.DateTimeField(null=True)
    time_added = models.DateTimeField(auto_now_add=True)
    post_assembly_filters = models.JSONField(null=True)
    additional_query_parameters = models.TextField(null=False, default=os.environ.get("ADDITIONAL_QUERY_PARAMS"))


class Records(models.Model):
    id = models.CharField(primary_key=True, max_length=LENGTH_MEDIUM)
    taxon = models.ForeignKey("Taxons", related_name="records", on_delete=models.DO_NOTHING)
    time_fetched = models.DateTimeField(auto_now_add=True)
    experiment_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    run_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    sample_accession = models.CharField(null=True, max_length=LENGTH_ACCESSION)
    fastq_ftp = models.CharField(null=True, max_length=LENGTH_LONG)
    lat = models.CharField(null=True, max_length=LENGTH_SHORT)
    lon = models.CharField(null=True, max_length=LENGTH_SHORT)
    country = models.CharField(null=True, max_length=LENGTH_LONG)
    collection_date = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    first_public = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    passed_filter = models.BooleanField(null=True, default=True)
    waiting_since = models.DateTimeField(null=True)
    assembly_result = models.CharField(
        choices=[(s.value, s.value) for s in AssemblyStatus],
        default=AssemblyStatus.WAITING.value,
        max_length=LENGTH_ACCESSION
    )
    passed_screening = models.BooleanField(null=True)
    screening_message = models.CharField(null=True, max_length=LENGTH_LONG)
    assembled_genome_url = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembled_genome_sha1 = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_error_report_url = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_error_process = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_error_exit_code = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_error_stdout = models.CharField(null=True, max_length=LENGTH_MEDIUM)
    assembly_error_stderr = models.CharField(null=True, max_length=LENGTH_MEDIUM)


qualifyr_name_map = {
    'sample_name': 'sample_name',
    'result':  'result',
    'bactinspector_result_metric_value':  'bactinspector.result.metric_value',
    'bactinspector_result_check_result':  'bactinspector.result.check_result',
    'bactinspector_species_metric_value':  'bactinspector.species.metric_value',
    'bactinspector_species_check_result':  'bactinspector.species.check_result',
    'confindr_contam_status_metric_value':  'confindr.contam_status.metric_value',
    'confindr_contam_status_check_result':  'confindr.contam_status.check_result',
    'confindr_percentage_contamination_metric_value':  'confindr.percentage_contamination.metric_value',
    'confindr_percentage_contamination_check_result':  'confindr.percentage_contamination.check_result',
    'fastqc_1_Adapter_Content_metric_value':  'fastqc 1.Adapter Content.metric_value',
    'fastqc_1_Adapter_Content_check_result':  'fastqc 1.Adapter Content.check_result',
    'fastqc_1_Basic_Statistics_metric_value':  'fastqc 1.Basic Statistics.metric_value',
    'fastqc_1_Basic_Statistics_check_result':  'fastqc 1.Basic Statistics.check_result',
    'fastqc_1_Overrepresented_sequences_metric_value':  'fastqc 1.Overrepresented sequences.metric_value',
    'fastqc_1_Overrepresented_sequences_check_result':  'fastqc 1.Overrepresented sequences.check_result',
    'fastqc_1_Per_base_N_content_metric_value':  'fastqc 1.Per base N content.metric_value',
    'fastqc_1_Per_base_N_content_check_result':  'fastqc 1.Per base N content.check_result',
    'fastqc_1_Per_base_sequence_quality_metric_value':  'fastqc 1.Per base sequence quality.metric_value',
    'fastqc_1_Per_base_sequence_quality_check_result':  'fastqc 1.Per base sequence quality.check_result',
    'fastqc_1_Per_sequence_GC_content_metric_value':  'fastqc 1.Per sequence GC content.metric_value',
    'fastqc_1_Per_sequence_GC_content_check_result':  'fastqc 1.Per sequence GC content.check_result',
    'fastqc_1_Per_sequence_quality_scores_metric_value':  'fastqc 1.Per sequence quality scores.metric_value',
    'fastqc_1_Per_sequence_quality_scores_check_result':  'fastqc 1.Per sequence quality scores.check_result',
    'fastqc_1_Sequence_Duplication_Levels_metric_value':  'fastqc 1.Sequence Duplication Levels.metric_value',
    'fastqc_1_Sequence_Duplication_Levels_check_result':  'fastqc 1.Sequence Duplication Levels.check_result',
    'fastqc_1_Sequence_Length_Distribution_metric_value':  'fastqc 1.Sequence Length Distribution.metric_value',
    'fastqc_1_Sequence_Length_Distribution_check_result':  'fastqc 1.Sequence Length Distribution.check_result',
    'fastqc_2_Adapter_Content_metric_value':  'fastqc 2.Adapter Content.metric_value',
    'fastqc_2_Adapter_Content_check_result':  'fastqc 2.Adapter Content.check_result',
    'fastqc_2_Basic_Statistics_metric_value':  'fastqc 2.Basic Statistics.metric_value',
    'fastqc_2_Basic_Statistics_check_result':  'fastqc 2.Basic Statistics.check_result',
    'fastqc_2_Overrepresented_sequences_metric_value':  'fastqc 2.Overrepresented sequences.metric_value',
    'fastqc_2_Overrepresented_sequences_check_result':  'fastqc 2.Overrepresented sequences.check_result',
    'fastqc_2_Per_base_N_content_metric_value':  'fastqc 2.Per base N content.metric_value',
    'fastqc_2_Per_base_N_content_check_result':  'fastqc 2.Per base N content.check_result',
    'fastqc_2_Per_base_sequence_quality_metric_value':  'fastqc 2.Per base sequence quality.metric_value',
    'fastqc_2_Per_base_sequence_quality_check_result':  'fastqc 2.Per base sequence quality.check_result',
    'fastqc_2_Per_sequence_GC_content_metric_value':  'fastqc 2.Per sequence GC content.metric_value',
    'fastqc_2_Per_sequence_GC_content_check_result':  'fastqc 2.Per sequence GC content.check_result',
    'fastqc_2_Per_sequence_quality_scores_metric_value':  'fastqc 2.Per sequence quality scores.metric_value',
    'fastqc_2_Per_sequence_quality_scores_check_result':  'fastqc 2.Per sequence quality scores.check_result',
    'fastqc_2_Sequence_Duplication_Levels_metric_value':  'fastqc 2.Sequence Duplication Levels.metric_value',
    'fastqc_2_Sequence_Duplication_Levels_check_result':  'fastqc 2.Sequence Duplication Levels.check_result',
    'fastqc_2_Sequence_Length_Distribution_metric_value':  'fastqc 2.Sequence Length Distribution.metric_value',
    'fastqc_2_Sequence_Length_Distribution_check_result':  'fastqc 2.Sequence Length Distribution.check_result',
    'file_size_check_size_metric_value':  'file_size_check.size.metric_value',
    'file_size_check_size_check_result':  'file_size_check.size.check_result',
    "quast_Ns_per_100_kbp_metric_value":  "quast.# N's per 100 kbp.metric_value",
    "quast_Ns_per_100_kbp_check_result":  "quast.# N's per 100 kbp.check_result",
    'quast_contigs_metric_value':  'quast.# contigs.metric_value',
    'quast_contigs_check_result':  'quast.# contigs.check_result',
    'quast_GC_metric_value':  'quast.GC (%).metric_value',
    'quast_GC_check_result':  'quast.GC (%).check_result',
    'quast_N50_metric_value':  'quast.N50.metric_value',
    'quast_N50_check_result':  'quast.N50.check_result',
    'quast_Total_length_metric_value':  'quast.Total length.metric_value',
    'quast_Total_length_check_result':  'quast.Total length.check_result'
}


def name_map(s: str, to_python: bool = True) -> str:
    if to_python:
        for k, v in qualifyr_name_map.items():
            if v == s:
                return k
        raise ValueError(f"Unknown assembler field: '{s}'.")
    else:
        if s in qualifyr_name_map.keys():
            return qualifyr_name_map[s]
        raise ValueError(f"Unknown pythonized assembler field: '{s}'.")


class QualifyrReport(models.Model):
    record = models.OneToOneField("Records", related_name="qualifyr_report", on_delete=models.DO_NOTHING)

    sample_name = models.CharField(max_length=LENGTH_SHORT, null=True)
    result = models.CharField(max_length=LENGTH_SHORT, null=True)
    bactinspector_result_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    bactinspector_result_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    bactinspector_species_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    bactinspector_species_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    confindr_contam_status_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    confindr_contam_status_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    confindr_percentage_contamination_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    confindr_percentage_contamination_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Adapter_Content_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Adapter_Content_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Basic_Statistics_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Basic_Statistics_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Overrepresented_sequences_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Overrepresented_sequences_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_base_N_content_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_base_N_content_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_base_sequence_quality_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_base_sequence_quality_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_sequence_GC_content_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_sequence_GC_content_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_sequence_quality_scores_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Per_sequence_quality_scores_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Sequence_Duplication_Levels_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Sequence_Duplication_Levels_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Sequence_Length_Distribution_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_1_Sequence_Length_Distribution_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Adapter_Content_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Adapter_Content_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Basic_Statistics_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Basic_Statistics_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Overrepresented_sequences_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Overrepresented_sequences_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_base_N_content_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_base_N_content_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_base_sequence_quality_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_base_sequence_quality_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_sequence_GC_content_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_sequence_GC_content_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_sequence_quality_scores_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Per_sequence_quality_scores_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Sequence_Duplication_Levels_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Sequence_Duplication_Levels_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Sequence_Length_Distribution_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    fastqc_2_Sequence_Length_Distribution_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    file_size_check_size_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    file_size_check_size_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_Ns_per_100_kbp_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_Ns_per_100_kbp_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_contigs_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_contigs_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_GC_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_GC_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_N50_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_N50_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_Total_length_metric_value = models.CharField(max_length=LENGTH_SHORT, null=True)
    quast_Total_length_check_result = models.CharField(max_length=LENGTH_SHORT, null=True)

