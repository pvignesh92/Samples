
package com.zebra.avp.source.siebel.siebelPrinter


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Logger
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import java.io.FileNotFoundException
import java.io.IOException
import java.io.StringWriter
import java.lang.Double
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import java.util.Date
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType
import java.util.GregorianCalendar
import java.util.Calendar
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

case class siebelSchema( Contract_num:String, Repair_serial_num:String, Repair_part_num:String, RMA_num:String, Line_num:String, Repair_line_type: String, RMA_created_date: String, status_change_date: String, activity_created: String, Activity_status: String,  Repair_tracking_num: String, Fault_desc: String, Resolution: String, Employee_location:String,  Ship_to_city: String, Ship_to_country: String, Ship_to_Postal_code: String, Return_location : String )

object siebelTest {

  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf().setAppName("siebelTest")
    val sc = new SparkContext(conf)
    
  
    val sqlContext: SQLContext = new HiveContext(sc)
    import sqlContext.implicits._

   val siebel_source = sc.textFile("vignesh/AVP_tweak2.csv")


//Parsing the data using CSV Reader for splitting by comma delimiter
val result = siebel_source.map{ line =>
val reader = new CSVReader(new StringReader(line));
reader.readNext();
}

//Schema for Siebel Repair data




// Mapping the source data to Schema and converting the RDD to dataframe

val siebelData = result.filter(x => !x.contains("Contract NUM")).map(x => siebelSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6), x(7),x(8),x(9),x(10),x(11) ,x(12),x(13),x(14),x(15), x(16),x(17))).toDF

// UDFs
 
//UDF to populate resolution values with colon delimiter

 def addColon(s:String) = {
 if (((s != null) && (s.toString.nonEmpty))) {
    s + ":" +s
   } else null
   }
   
 //UDF to add NULL string to fault desc. values with colon delimiter

 def addNULL(s:String) = {
 if (((s != null) && (s.toString.nonEmpty))) {
    "NULL" + ":" +s
   } else null
   } 
   
   
//UDF to create pipe delimiter while grouping multiple resolution and fault desc values

def addPipe(s: scala.collection.mutable.WrappedArray[String]):String = {
    s.mkString("|")
   }

//Logic to populate values only if the status is marked as closed

def checkClosed(value:String,status:String ) = {
  if (status.equalsIgnoreCase("Closed")) {
    value
   } else null
   } 
      

//Registering UDFs    
 sqlContext.udf.register("addColon",addColon(_:String))
 sqlContext.udf.register("addNULL",addNULL(_:String))
 sqlContext.udf.register("addPipe",addPipe(_:scala.collection.mutable.WrappedArray[String]))
 sqlContext.udf.register("checkClosed",checkClosed(_:String,_:String))
 
// Dropping the records which do not have Contract Number
val siebelValidData = siebelData.filter("Contract_num != ''")

//Registering the output as temporary table for analysing in SQLContext
siebelValidData.registerTempTable("temp_data")

//Splitting the records of Resolution and Fault_desc keyed by Contract_num for further logics
val resolutionColumn = sqlContext.sql("select RMA_num, Resolution from temp_data")
val faultDescColumn = sqlContext.sql("select RMA_num, Fault_desc from temp_data")

//Applying the logic of colon and pipe 
val resolution_final = sqlContext.sql("select RMA_num as RMA_number, addPipe(COLLECT_SET(addColon(Resolution))) AS fault_category_code from temp_data GROUP BY RMA_num")

val faultDesc_final = sqlContext.sql("select RMA_num as RMA_number, addPipe(COLLECT_SET(addColon(Fault_desc))) AS prob_category_code from temp_data GROUP BY RMA_num")


//Splitting the records excluding Resolution and Fault_desc columns for further logics
val siebelColumns = sqlContext.sql("select Contract_num, Repair_serial_num, Repair_part_num, RMA_num, Line_num, Repair_line_type, RMA_created_date, status_change_date, activity_created, Activity_status, Repair_tracking_num, Employee_location, Ship_to_city, Ship_to_country, Ship_to_Postal_code, Return_location from temp_data")

//Registering the output as temporary table for analysing in SQLContext
siebelColumns.registerTempTable("siebel_repair")

//Logic to generate columns for Cassandra mapping 

val siebelColumns_final = sqlContext.sql("select Contract_num,Repair_serial_num , RMA_num, year(to_date(RMA_created_date)) AS rma_open_date, year(to_date(status_change_date)) AS rma_closed_date,  CONCAT(RMA_num,'-', Line_num) as ir_num, ' ' AS carrier_code, 'BRONZE' AS contract_type_code, CONCAT(Ship_to_city,' ',Ship_to_country,' ', Ship_to_Postal_code) AS delivered_site_address, '' AS delivered_site_id , 'NEED TO DO' AS delivered_site_name ,  status_change_date AS device_last_update_datetime, '' AS due_datetime, '' AS fault_code, '' AS job_category_code, '' AS load_type_code, Repair_part_num AS model_num,  '' AS problem_code, activity_created AS receive_datetime, Repair_serial_num AS received_prod_serial_num , checkClosed(status_change_date,Activity_status) AS repair_datetime, '' AS repair_status_code, 'Y' AS required_flag , Repair_serial_num AS return_prod_serialnum, checkClosed(status_change_date,Activity_status) AS rma_close_datetime, RMA_created_date  AS rma_open_datetime, checkClosed(status_change_date,Activity_status) AS shipped_datetime, '' AS sla_due_back, 'SIEBEL' AS source_code, '' AS spare_pool_date_seq_num, Repair_tracking_num AS tracking_num  from siebel_repair")

siebelColumns_final.registerTempTable("siebel_repair_source")

//Logic for comparing the rma_open_date and rma_closed_date to generate year_id
// Storing all the records where rma open year and closed year or same or rma yet to be closed
val sameYearOrNull = sqlContext.sql("select Contract_num, Repair_serial_num , RMA_num, rma_open_date AS year_id, ir_num, carrier_code, contract_type_code, delivered_site_address, delivered_site_id, delivered_site_name, device_last_update_datetime, due_datetime, fault_code, job_category_code, load_type_code, model_num, problem_code, receive_datetime, received_prod_serial_num, repair_datetime, repair_status_code, required_flag, return_prod_serialnum, rma_close_datetime, rma_open_datetime, shipped_datetime, sla_due_back, source_code, spare_pool_date_seq_num, tracking_num from siebel_repair_source")

// Storing all the records where rma open year and closed year is different

val diffYear = sqlContext.sql("select Contract_num, Repair_serial_num , RMA_num, rma_closed_date AS year_id, ir_num, carrier_code, contract_type_code, delivered_site_address, delivered_site_id, delivered_site_name, device_last_update_datetime, due_datetime, fault_code, job_category_code, load_type_code, model_num, problem_code, receive_datetime, received_prod_serial_num, repair_datetime, repair_status_code, required_flag, return_prod_serialnum, rma_close_datetime, rma_open_datetime, shipped_datetime, sla_due_back, source_code, spare_pool_date_seq_num, tracking_num from siebel_repair_source where rma_open_date != rma_closed_date AND rma_closed_date is not null")

// Doing Union to generate the logic -  case got open on 2016 but got closed in 2017, in that same records with there for both the year id //2016 and 2017.
val siebelBeforeJoin = sameYearOrNull.unionAll(diffYear)

//Joining the datasets to create final column structure

val siebelJoinResolution = siebelBeforeJoin.join(resolution_final, siebelBeforeJoin("RMA_num") === resolution_final("RMA_number"), "inner").drop(resolution_final("RMA_number"))

val siebelFinal = siebelJoinResolution.join(faultDesc_final, siebelJoinResolution("RMA_num") === faultDesc_final("RMA_number"), "inner")drop(faultDesc_final("RMA_number"))

siebelFinal.registerTempTable("siebelFinal")

siebelFinal.saveAsTable("pvignesh.siebel_8_21")

val siebelRDD = siebelFinal.rdd


siebelRDD.coalesce(1).saveAsTextFile("vignesh/siebelResults_Check1")

    //Stop the Spark context
    sc.stop

  }

}