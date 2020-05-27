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
		
		List<Row> inMemory = new ArrayList<Row>();
		
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};
		
		StructType schema = new StructType(fields);
		Dataset<Row> dataset = spark.createDataFrame(inMemory, schema );
		
		dataset.show();
		
		//Dataset<Row> modernArtResults = dataset.filter( (FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art") 
		//		&& Integer.parseInt(row.getAs("year")) >= 2007);
		
		// temporary view
		//dataset.createOrReplaceTempView("my_students_table");
		
		//Dataset<Row> results = spark.sql("select score, year from my_students_table where subject='French'");
		//Dataset<Row> results = spark.sql("select max(score) from my_students_table where subject='French'");
		//Dataset<Row> results = spark.sql("select avg(score) from my_students_table where subject='French'");
		//Dataset<Row> results = spark.sql("select distinct(year) from my_students_table where subject='French' order by year");
		//Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
		
		//results.show();
		
		spark.close();
		

	}

}
