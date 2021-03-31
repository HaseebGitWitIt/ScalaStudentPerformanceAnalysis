import org.apache.spark.sql.SparkSession
import spark.implicits._
import org.apache.spark.sql.functions._

object StudentAnalysis {
        def main(args : Array[String]) : Unit = {
                val spark = SparkSession.builder().master("localhost").appName("AppName").getOrCreate();
                val origData = spark.read.csv("StudentsPerformance.csv");
                val data = origData.withColumnRenamed("_c0","Gender").withColumnRenamed("_c1","Race/Ethnicity").withColumnRenamed("_c2","Parent_Education").withColumnRenamed("_c3","Lunch").withColumnRenamed("_c4","Test_Preparation").withColumnRenamed("_c5","Math_Score").withColumnRenamed("_c6", "Reading_Score").withColumnRenamed("_c7","Writing_Score");
                println("----------------------------------------------A in Everything--------------------------------")
                data.filter("Math_Score > 80 AND Reading_Score > 80 AND Writing_Score > 80").show(5);
                println("----------------------------------------------A in Something--------------------------------")
                data.filter("Math_Score > 80 OR Reading_Score > 80 OR Writing_Score > 80").show(5);
                println("----------------------------------------------Average A--------------------------------")
                data.filter("((Math_Score+Reading_Score+Writing_Score)/3) > 80").show(5);
                println("----------------------------------------------Average Scores Male vs Female------------------")
                data.groupBy("Gender").agg(avg("Math_Score"),avg("Reading_Score"),avg("Writing_Score")).show();
                println("----------------------------------------------Average Scores Based on Parent Education-------")
                data.createOrReplaceTempView("grades")
                spark.sql("SELECT Parent_Education,(Math_Score+Reading_Score+Writing_Score)/3 as Average_Score FROM grades GROUP BY Parent_Education,Math_Score,Reading_Score,Writing_Score").show(5)
                println("----------------------------------------------Average Scores Prepared vs Non-Prepared--------")
                data.groupBy("Test_Preparation").agg(avg("Math_Score"),avg("Reading_Score"),avg("Writing_Score")).show();
        }
}
/*
        Test.main(Array(null))
*/
