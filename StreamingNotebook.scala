// Databricks notebook source
/**
case classes which is use to hold immuatable Input Data (Loan and Account)
**/
case class Account(accountid:Long, accounttype:Int, event_time: java.sql.Timestamp) {
    def this() = this(0L, 0 , new java.sql.Timestamp(0L))
}
case class Loan(loanid:Long, accountid:Long, amount:Double , event_time: java.sql.Timestamp) {
    def this() = this(0L, 0 , 0.0, new java.sql.Timestamp(0L))
}


// COMMAND ----------

/** Static variables used through out the program. */
object Variables {
  val r = new scala.util.Random
  val ACCOUNT_TYPES = Seq(1, 2, 3, 4)
  val accTypeLength = ACCOUNT_TYPES.length
  val accids = 1 to 500000 by 2
}

// COMMAND ----------

/* start the generate data streams for Loan Account sources*/

import java.util.UUID
import org.apache.spark.sql.functions._

val populateAccount = udf((i: Int) => {
  val typeIndex = Variables.r.nextInt(Variables.accTypeLength)  
  Account(Variables.accids(i) , Variables.ACCOUNT_TYPES(typeIndex) , new java.sql.Timestamp(System.currentTimeMillis) )
})

val accounts = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 2)
      .option("numPartitions", 1)
      .load()
      .select(
        populateAccount('value) as 'account)


display(accounts)

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.functions._

val populateLoan = udf((i: Int) => {
Loan(i , Variables.accids(i), i * 10.00 , new java.sql.Timestamp(System.currentTimeMillis) ) 
})


val loans = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 2)
      .option("numPartitions", 1)
      .load()
      .select(
        //lit('value) as 'val,
        populateLoan('value) as 'loan
        
        )

//display(loans)

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._


val loanStream = loans.select($"loan.*")
.withColumnRenamed("accountid" , "loan_accountid")
.withColumnRenamed("event_time" , "loan_event_time")
//.withColumn("Date", unix_timestamp('loan_event_time, "EEE MMM dd HH:mm:ss zzz yyyy").cast(TimestampType))

val accountStream = accounts.select($"account.*")

//display(loanStream)


// COMMAND ----------


// Join with event-time constraints

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql._

import spark.implicits._

var query = loanStream
.join(  accountStream, loanStream("loan_accountid") === accountStream("accountid"))
.withWatermark("loan_event_time", "1 minute")
.groupBy(window($"loan_event_time", "1 minutes" , "1 minute") as 'time_window , $"accounttype" as 'AccountType)
.agg(count("*") as 'TotalCount, sum("amount") as 'TotalAmount)
//display(query)



// COMMAND ----------


import scala.concurrent.duration._

query
.writeStream
//.option("checkpointLocation", s"/tmp/state1")
//.outputMode("Append")
.foreachBatch { (df: Dataset[Row], batchId: Long) => {   //foreachBatch() returns type DataStreamWriter
    println("batchId" + batchId)
    var ts = new java.sql.Timestamp(System.currentTimeMillis)
    println(ts)
     //   df.show(false)
    
  if(!df.rdd.isEmpty){
      df.show(false)
    }
  }
}
.trigger(Trigger.ProcessingTime(60.seconds))
.start().awaitTermination()              // start() returns type StreamingQuery

