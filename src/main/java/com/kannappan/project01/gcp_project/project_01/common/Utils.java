package com.kannappan.project01.gcp_project.project_01.common;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.List;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.FileCopyUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {

	public static String getSql(String path) {
		String sql = null;
		ResourceLoader resourceLoader = new DefaultResourceLoader();
		Resource resource = resourceLoader.getResource(path);
		try (Reader reader = new InputStreamReader(resource.getInputStream(), UTF_8)) {
			sql = FileCopyUtils.copyToString(reader);
			sql = sql.replaceAll("\n", "\t");
			sql = sql.replaceAll("\r", "\t");
			log.debug("path = " + path + " sql = " + sql);
			return sql;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static String parseAsCsv(List<String> dataDomainList) {
		String dataDomainCsv = dataDomainList.stream().filter(s -> s != null && !"".equals(s)).distinct()
				.collect(joining(","));
		return dataDomainCsv;
	}

}
