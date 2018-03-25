package com.wikibooks.spark.ch5;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * 파이썬에서 사용할 UDF 함수 . dataframe_sample.py 의 run_java_udf 함수 참고    
 */
public class Judf implements UDF1<String, Boolean> {
	@Override
	public Boolean call(String job) throws Exception {
		return StringUtils.equals(job, "student");
	}
}
