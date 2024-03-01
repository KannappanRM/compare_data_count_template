package com.kannappan.project01.gcp_project.project_01.common;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryService {

	static String logMsg = "[kannappan] [ActivityCount] [BigQuery] jobId : %s executing query : %s";
	static String logMsg1 = "[kannappan] [ActivityCount] [BigQuery] jobId : %s succeeded. totalRows returned : %s";
	static String errMsg = "[kannappan] [ActivityCount] [BigQuery] jobId : %s failed. query : %s Reason : %s";
	static String batchInsertInfoMsg = "[kannappan] [ActivityCount] [BigQuery] Batch Insert to table %s succeeded. totalRows inserted : %s";
	static String batchInsertErrMsg = "[kannappan] [ActivityCount] [BigQuery] Batch Insert to table %s failed with following errors.";
	static String batchInsertExcMsg = "[kannappan] [ActivityCount] [BigQuery] Batch Insert to table %s failed. Exception : %s";
	static String streamingInsertInfoMsg = "[kannappan] [ActivityCount] [BigQuery] Streaming Insert to table %s succeeded. bqRow : %s";
	static String streamingInsertErrMsg = "[kannappan] [ActivityCount] [BigQuery] Streaming Insert to table %s failed with following errors. bqRow : %s";
	static String streamingInsertExcMsg = "[kannappan] [ActivityCount] [BigQuery] Streaming Insert to table %s failed. bqRow : %s Exception : %s";

	public static TableResult execute(String kannappanProject, String kannappanDataset, String query) throws Exception {
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(kannappanProject).build().getService();
		query = query.replaceAll("\n", "\t");
		query = query.replaceAll("\r", "\t");
		JobId jobId = null;
		try {
			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setDefaultDataset(kannappanDataset)
					.setUseLegacySql(false).build();
			jobId = JobId.of(kannappanProject, "kannappan_activity_count_" + UUID.randomUUID().toString());
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
			log.info(String.format(logMsg, jobId.getJob(), query));
			queryJob = queryJob.waitFor();
			if (queryJob == null) {
				throw new RuntimeException("Job no longer exists");
			} else if (queryJob.getStatus() != null && queryJob.getStatus().getError() != null) {
				throw new RuntimeException(queryJob.getStatus().getError().toString());
			}
			TableResult result = queryJob.getQueryResults();
			log.info(String.format(logMsg1, jobId.getJob(), result.getTotalRows()));
			return result;
		} catch (Exception ex) {
			log.error(String.format(errMsg, jobId.getJob(), query, ex));
			throw new RuntimeException(ex);
		}
	}

	public static void insertRow(String kannappanProject, String kannappanDataset, String table, Map<String, Object> bqRow)
			throws Exception {
		try {
			bqRow.entrySet().removeIf(entry -> (null == entry.getKey() || null == entry.getValue()));
			BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(kannappanProject).build().getService();
			InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(TableId.of(kannappanDataset, table))
					.setRows(ImmutableList.of(InsertAllRequest.RowToInsert.of(UUID.randomUUID().toString(), bqRow)))
					.build());
			if (response.hasErrors()) {
				log.error(String.format(streamingInsertErrMsg, table, bqRow));
				for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
					log.error("BigQueryError >> " + entry.getKey() + " : " + entry.getValue());
				}
			} else {
				log.info(String.format(streamingInsertInfoMsg, table, bqRow));
			}
		} catch (BigQueryException ex) {
			log.error(String.format(streamingInsertExcMsg, table, bqRow, ex));
		}
	}

	public static void insertRows(String kannappanProject, String kannappanDataset, String table, Queue<Map<String, Object>> bqRows)
			throws Exception {
		try {
			List<InsertAllRequest.RowToInsert> rowsToInsert = bqRows.stream()
					.map(bqRow -> InsertAllRequest.RowToInsert.of(UUID.randomUUID().toString(), bqRow))
					.collect(Collectors.toList());
			log.debug("bqRows : " + bqRows);
			log.debug("rowsToInsert : " + rowsToInsert);
			BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(kannappanProject).build().getService();
			InsertAllResponse response = bigquery.insertAll(
					InsertAllRequest.newBuilder(TableId.of(kannappanDataset, table)).setRows(rowsToInsert).build());
			if (response.hasErrors()) {
				log.error(String.format(batchInsertErrMsg, table));
				for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
					log.error("BigQueryError >> " + entry.getKey() + " : " + entry.getValue());
				}
			} else {
				log.info(String.format(batchInsertInfoMsg, table, bqRows.size()));
			}
		} catch (BigQueryException ex) {
			log.error(String.format(batchInsertExcMsg, table, ex));
		}
	}

}
