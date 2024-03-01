package com.kannappan.project01.gcp_project.project_01.kannappanCount;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

import javax.net.ssl.HttpsURLConnection;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kannappan.project01.gcp_project.project_01.common.BigQueryService;
import com.kannappan.project01.gcp_project.project_01.common.Utils;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class KannappanCountController {

	@Value("${kannappan.project}")
	private String kannappanProject;

	@Value("${kannappan.dataset}")
	private String kannappanDataset;

	@Value("${kannappan.activity.count.table}")
	private String kannappanActivityCountTable;

	@Value("${splunk.url.search}")
	private String splunkSearchUrl;

	@Value("${splunk.url.search.result}")
	private String splunkSearchResultUrl;

	@Value("${splunk.search.query}")
	private String splunkSearchQuery;

	@Value("${splunk.token.secretId}")
	private String splunkSecretId;

	@Value("${splunk.token.versionId}")
	private String splunkVersionId;

	static String countMatchedMsg = "[kannappan] [ActivityCount] GCP vs Splunk Count Matched. gcpCount : {}, splunkCount : {}";
	static String countNotMatchedMsg = "[Alerts] [kannappan] [ActivityCount] GCP vs Splunk Count Not Matched. gcpCount : {}, splunkCount : {}";

	@PostMapping(value = "/")
	public void getActivityCount() throws Exception {

		log.info("[kannappan] [ActivityCount] Started executing...");

		LocalDateTime estTimeYest = getYesterdayTime();
		Long gcpCount = getGcpCount(estTimeYest.toString());
		Long splunkCount = getSplunkCount();
		insertActivityCounts(estTimeYest, gcpCount, splunkCount);

		if (!gcpCount.equals(splunkCount)) {
			log.info(countNotMatchedMsg, gcpCount, splunkCount);
		} else {
			log.info(countMatchedMsg, gcpCount, splunkCount);
		}

		log.info("[kannappan] [ActivityCount] Finished executing...");

	}

	private LocalDateTime getYesterdayTime() {
		LocalDateTime utcTime = LocalDateTime.now(ZoneOffset.UTC);
		ZoneId estZone = ZoneId.of("America/New_York");
		ZoneId utcZone = ZoneId.of("UTC");
		LocalDateTime estTimeNow = utcTime.atZone(utcZone).withZoneSameInstant(estZone).toLocalDateTime();
		LocalDateTime estTimeYest = estTimeNow.minus(24, ChronoUnit.HOURS);
		return estTimeYest;
	}

	private long getSplunkCount() throws Exception {
		log.info(splunkSearchQuery);
		URL searchUrl = new URL(splunkSearchUrl);
		HttpsURLConnection searchHttpConn = (HttpsURLConnection) searchUrl.openConnection();
		searchHttpConn.setRequestMethod("POST");
		searchHttpConn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		searchHttpConn.setRequestProperty("Authorization", getSecret());
		searchHttpConn.setDoOutput(true);
		OutputStreamWriter writer = new OutputStreamWriter(searchHttpConn.getOutputStream());
		writer.write(splunkSearchQuery);
		writer.flush();
		writer.close();
		searchHttpConn.getOutputStream().close();

		InputStream searchResponseStream = searchHttpConn.getResponseCode() / 100 == 2 ? searchHttpConn.getInputStream()
				: searchHttpConn.getErrorStream();
		InputStreamReader inputStreamReader = new InputStreamReader(searchResponseStream);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		StringBuffer stringBuffer = new StringBuffer();
		String readLine = bufferedReader.readLine();
		while (readLine != null) {
			stringBuffer.append(readLine);
			readLine = bufferedReader.readLine();
		}
		log.info("[kannappan] [ActivityCount] searchResponse: {}", stringBuffer.toString());

		// {"sid":"9234950234.5500334_DA2BF363-3387-4240-ADBC-45386670B18C"}
		JSONObject result = new JSONObject(stringBuffer.toString());
		String sid = result.getString("sid");

		// Let the search finish before getting results
		Thread.sleep(10000);

		String resultUrlStr = String.format(splunkSearchResultUrl, sid);
		// Get request to get the results
		log.info("[kannappan] [ActivityCount] resultUrlStr: {}", resultUrlStr);

		URL resultUrl = new URL(resultUrlStr);
		HttpURLConnection resultHttpConn = (HttpURLConnection) resultUrl.openConnection();
		resultHttpConn.setRequestMethod("GET");
		resultHttpConn.setRequestProperty("Authorization", getSecret());

		InputStream resultResponseStream = resultHttpConn.getResponseCode() / 100 == 2 ? resultHttpConn.getInputStream()
				: resultHttpConn.getErrorStream();
		Scanner resultScanner = new Scanner(resultResponseStream).useDelimiter("\\A");
		String resultResponse = resultScanner.hasNext() ? resultScanner.next() : "";
		log.info("[kannappan] [ActivityCount] resultResponse: {}", resultResponse);
		JSONObject searchResultJson = new JSONObject(resultResponse);
		String splunkCount = searchResultJson.getJSONArray("results").getJSONObject(0).getString("count");
		log.info("[kannappan] [ActivityCount] splunkCount: {}", splunkCount);

		return Integer.parseInt(splunkCount);
	}

	private long getGcpCount(String estTimeYest) throws Exception {

		long gcpCount = 0;
		String countQuery = String.format(Utils.getSql("/sql/kannappan.activity.log.count.sql"), estTimeYest);
		TableResult result = BigQueryService.execute(kannappanProject, kannappanDataset, countQuery);
		for (FieldValueList row : result.iterateAll()) {
			gcpCount = row.get("count").getLongValue();
		}
		log.info("[kannappan] [ActivityCount] gcpCount: {}", gcpCount);
		return gcpCount;
	}

	private void insertActivityCounts(LocalDateTime estTimeYest, Long gcpCount, Long splunkCount) throws Exception {

		String createQuery = Utils.getSql("/sql/kannappan.activity.count.create.sql");
		BigQueryService.execute(kannappanProject, kannappanDataset, createQuery);
		String estDateYest = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH).format(estTimeYest);

		Map<String, Object> rowContent = new HashMap<>();
		rowContent.put("Recorded_date", estDateYest);
		rowContent.put("Dataflow_activity_count", gcpCount);
		rowContent.put("Splunk_activity_count", splunkCount);

		BigQueryService.insertRow(kannappanProject, kannappanDataset, kannappanActivityCountTable, rowContent);
		log.info("[kannappan] [ActivityCount] Inserted Activity Counts...");

	}

	private String getSecret() {
		log.info("Generating GCP secret payload for projectId:{}, secretId:{}, secretVersionId:{}", kannappanProject,
				splunkSecretId, splunkVersionId);
		try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {

			SecretVersionName secretVersionName = SecretVersionName.of(kannappanProject, splunkSecretId, splunkVersionId);
			AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
			String splunkSecretPayload = response.getPayload().getData().toStringUtf8();
			log.info("[kannappan] [ActivityCount] [GetGcpSecret] payload generated as:{}", splunkSecretPayload);
			return splunkSecretPayload;
		} catch (Exception e) {
			log.error(e.toString());
			e.printStackTrace();
		}
		log.info(
				"[kannappan] [ActivityCount] [GetGcpSecret] unable to generate payload for projectId:{}, secretId:{}, secretVersionId:{}",
				kannappanProject, splunkSecretId, splunkVersionId);
		return "";
	}

}
