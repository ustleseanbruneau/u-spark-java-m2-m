package com.leseanbruneau;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

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
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// dataset.show() will default top 20 results
		//dataset.show();
		
		//long numberOfRows = dataset.count();
		//System.out.println("There are " + numberOfRows + " records");
		
		//Row firstRow = dataset.first();
		
		//String subject = (String) firstRow.get(2).toString();
		//String subject = (String) firstRow.getAs("subject").toString();
		//System.out.println(subject);
		
		//int year = Integer.parseInt(firstRow.getAs("year"));
		//System.out.println("The year was: " + year);
		
		// First approach
		//Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' ");
		//Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007 ");
		
		// Second approach
		//Dataset<Row> modernArtResults = dataset.filter( (FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art") 
		//		&& Integer.parseInt(row.getAs("year")) >= 2007);
		
		// Third approach
		//Column subjectColumn = dataset.col("subject");
		//Column yearColumn = dataset.col("year");
		//Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
		//		.and(yearColumn.geq(2007)));
		
		
		//Fourth approach
		//Column subjectColumn = functions.col("subject");
		
		// import static org.apache.spark.sql.functions.*;
		// line would be
		//  Column subjectColumn = col("subject");
		// or
		//  no Column definitions
		Dataset<Row> modernArtResults = 
		         dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		
		// This will shuffle data around, load into memory
		modernArtResults.show();
		
		spark.close();
		

	}

}
