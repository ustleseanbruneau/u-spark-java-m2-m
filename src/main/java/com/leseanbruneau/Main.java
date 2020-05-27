package com.leseanbruneau;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

	public static void main(String[] args) {
		
		// To set Windows Environment variable
		//System.setProperty("hadoop.home.dir", "c:/hadoop");		
		//Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// Note: Windows need to add .config() for directory, windows directory does not have to exist to run
		//SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
		//		.config("spark.sql.warehouse.dir","file:///c:/temp/spark")
		//		.getOrCreate();
		
		
		
		// Run on Linux		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.getOrCreate();
		
		//Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
//		List<Row> inMemory = new ArrayList<Row>();
//		
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
//		StructField[] fields = new StructField[] {
//				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
//		};
//		
//		StructType schema = new StructType(fields);
//		Dataset<Row> dataset = spark.createDataFrame(inMemory, schema );
		
		// Chapter 66 - Multiple Grouping
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		dataset.createOrReplaceTempView("logging_table");
		
		//  Section 64 - Group By
		//Dataset<Row> results = spark.sql("select level, count(datetime) from logging_table group by level order by level");
		//Dataset<Row> results = spark.sql("select level, collect_list(datetime) from logging_table group by level order by level");
		
		// Section 65 - Date Formatting
		//Dataset<Row> results = spark.sql("select level, date_format(datetime, 'yyyy') from logging_table");
		// 'M' - number, no leading zero
		//Dataset<Row> results = spark.sql("select level, date_format(datetime, 'M') from logging_table");
		// 'MM' - number with leading zero
		//Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MM') from logging_table");
		// 'MMM' - letters, short month name
		//Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMM') from logging_table");
		// 'MMMM' - letters, month spelled out
		//Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");
		
		// Section 66 - Multiple Groupings
		//results.createOrReplaceTempView("logging_table");
		//results = spark.sql("select level, month, count(1) as total from logging_table group by level, month");
		
		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month");
		
		
		//results.show();
		// show up to 100 rows
		results.show(100);
		
		results.createOrReplaceTempView("results_table");
		
		Dataset<Row> totals = spark.sql("select sum(total) from results_table");
		totals.show();
		
		spark.close();
		

	}

}
