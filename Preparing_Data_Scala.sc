/*
This code uses Spark and prepares the data.It finds unplanned 30-day re-admissions then concatenate the admission table with
discharge summaries and write it to the directory called "Prepared-Data".
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
val spark = SparkSession.builder.
  master("local")
  .appName("spark session example")
  .getOrCreate()

import org.apache.spark.sql.functions._
import spark.implicits._

val Data = spark.read.option("header", "true").option("inferSchema", "true").csv("ADMISSIONS.csv")

Data.show(2)
Data.printSchema()
Data.count()

val Data2 = Data.groupBy("SUBJECT_ID").count()
Data2.show()
Data2.select(max("count")).show()
Data2.select(mean("count")).show()
val partitionWindow = Window.partitionBy($"SUBJECT_ID").orderBy($"ADMITTIME")
val rankTest = rank().over(partitionWindow)
val Data3 = Data.select($"*", rankTest as "RANK")
Data3.printSchema()
val Data4 = Data3.select(Data3("SUBJECT_ID"), Data3("HADM_ID"), Data3("ADMITTIME"), Data3("RANK"), Data3("ADMISSION_TYPE"))
val leadTest = lead($"ADMITTIME", 1, null).over(partitionWindow)
val Data5 = Data4.select($"*", leadTest as "NEXT_VALUE")
Data5.show()
val Data6 = Data5.withColumn("TIME_DIFFERENCE", datediff($"NEXT_VALUE",$"ADMITTIME"))
val leadTest2 = lead($"ADMISSION_TYPE", 1, null).over(partitionWindow)
val Data7 = Data6.select($"*", leadTest2 as "NEXT_ADMISSION_TYPE")
Data7.show(100)
Data7.printSchema()
def createResponse(time_difference: Double , next_admission_type: String ): Int = {
  if((time_difference <=1) && (next_admission_type != "ELECTIVE" )) 1
  else 0
}
createResponse(2, "nok")
createResponse(.5, "ELECTIVE")
createResponse(.4, "whatever")
Data7.select($"ADMISSION_TYPE").distinct().show()
val Data8_0 = Data7.filter( Data7("ADMISSION_TYPE") !== "NEWBORN")
Data8_0.select(("ADMISSION_TYPE")).distinct().show()
val Data8 = Data8_0.withColumn("RESPONSE", when((($"TIME_DIFFERENCE" <= 40) && ($"NEXT_ADMISSION_TYPE" !== "ELECTIVE" )), 1).otherwise(0))
Data8.show(100)
Data8.select($"ADMISSION_TYPE").distinct().show()
val Data9 = Data8.select( $"HADM_ID", $"RESPONSE")
Data9.show()
Data9.count()
val Data10= Data9.groupBy("RESPONSE").count()
Data10.select("count").distinct().show()
val Note = spark.read.option("header", "true").option("mode", "FAILFAST").option("escape", "\"")
  .option("multiline", "true").option("inferSchema", "true").csv("NOTEEVENTS.csv")
Note.printSchema()
val Discharge_Note = Note.filter($"CATEGORY" === "Discharge summary")
Discharge_Note.show()
val DN_C = Discharge_Note.groupBy("HADM_ID").count()
DN_C.select("count").distinct().show()
val partitionWindow2 = Window.partitionBy($"HADM_ID").orderBy($"CHARTDATE".desc)
val rankTest2 = rank().over(partitionWindow2)
val Last_Discharge_Note = Discharge_Note.withColumn("ROW_NUM", row_number().over(partitionWindow2)).where($"ROW_NUM" ===1)
Last_Discharge_Note.show()
Last_Discharge_Note.select($"ROW_NUM").distinct().show()
val Discharge_Text = Last_Discharge_Note.select($"SUBJECT_ID", $"HADM_ID", $"TEXT")
Discharge_Text.show()
val Final_Dataset = Discharge_Text.join(Data9, "HADM_ID")
Final_Dataset.show(1, false)
Final_Dataset.count()
//Final_Dataset.write.format("csv").save("C:/USF/619/Prepared-Data")


