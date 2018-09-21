package com.sparktest

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

object UbsExercise {

  var sparkSes: SparkSession = null
  def main(args: Array[String]) {    
    try{
      initiateSparkSession()    
      processBusinessLogic()
    }catch{
      case t: Throwable => t.printStackTrace() 
    }finally{
      if(sparkSes!=null)
      sparkSes.stop()
    }  
    
  }

  def initiateSparkSession(){
    if(sparkSes==null){
      val str = "local[2]" //using 2 cores of local machine(ie two threads)
      val sparkConf = new SparkConf().setMaster(str).setAppName("UbsExercise")
      
      var ssb = SparkSession.builder.config(sparkConf)
      sparkSes = ssb.getOrCreate()
      sparkSes.sparkContext.setLogLevel("ERROR") // log level error
    }else{
      //do nothing as spark session is already initialized
    }
  }
  
  def getSparkSession(): SparkSession= {
    sparkSes
  }
  
  def processBusinessLogic(){
     
    val sparkS = getSparkSession()    
    import sparkS.implicits._   
    
    val startOfDayPosStruct = ScalaReflection.schemaFor[StartOfDayPosCase].dataType.asInstanceOf[StructType]
    val txnStruct = ScalaReflection.schemaFor[TxnCase].dataType.asInstanceOf[StructType]
    
    var startOfDayPosDf = readCsvFile("src/main/resources/Input_StartOfDay_Positions.txt", startOfDayPosStruct)      
    var txnDf = readJsonFile("src/main/resources/1537277231233_Input_Transactions.txt", txnStruct)
    
    txnDf = txnDf.withColumn("Buy", when('TransactionType.equalTo("B"), 'TransactionQuantity).otherwise(0))
                  .withColumn("Sell", when('TransactionType.equalTo("S"), 'TransactionQuantity).otherwise(0))     
     
    //creating instrument wise total buy and total sell columns
    txnDf = txnDf.groupBy('Instrument).agg(sum('Buy) as "totalBuy", sum('Sell) as "totalSell")
    
    //join position and txn DFs to find updated quantity and Delta
    var endOfDayPosDf =  startOfDayPosDf.join(txnDf, Seq("Instrument"), "LeftOuter")
                        .withColumn("Quantity", 
                             when('AccountType.equalTo("E") && 'totalBuy.isNotNull && 'totalSell.isNotNull, 'Quantity + 'totalBuy - 'totalSell)
                            .otherwise(when('AccountType.equalTo("I") && 'totalBuy.isNotNull && 'totalSell.isNotNull, 'Quantity - 'totalBuy + 'totalSell).otherwise('Quantity)))
                        .withColumn("Delta", 
                             when('AccountType.equalTo("E") && 'totalBuy.isNotNull && 'totalSell.isNotNull, 'totalBuy - 'totalSell)
                            .otherwise(when('AccountType.equalTo("I") && 'totalBuy.isNotNull && 'totalSell.isNotNull, 'totalSell - 'totalBuy).otherwise(0)))
                        .drop('totalBuy).drop('totalSell)
    
    endOfDayPosDf = orderDfByDeltaValue(endOfDayPosDf)                      
                      
    writeOutput(endOfDayPosDf, "src/main/resources/Expected_EndOfDay_Positions.csv")                
  }
  
  def readCsvFile(filePath: String, schema: StructType): DataFrame= {    
    var df: DataFrame = null
    if(filePath!=null && schema!=null){
      val sparkS = getSparkSession()
      df = sparkS.read
        .format("csv")
        .option("header", true)
        .option("delimiter", ",")      
        .option("inferSchema", "true")
        .schema(schema)
        .load(filePath)      
    }else{
      throw new RuntimeException("please provide file path and schema for readCsvFile")
    }
    df
  }
  
  def readJsonFile(filePath: String, schema: StructType): DataFrame= {
    //reading 1537277231233_Input_Transactions.txt file and converting json to single line json as after spark 2.3 json should be in single line
    var df: DataFrame = null
    if(filePath!=null && schema!=null){
      val sparkS = getSparkSession()
      val json = sparkS.sparkContext.wholeTextFiles(filePath).map(tuple => tuple._2.replace("\n", "").trim)
      df = sparkS.read.schema(schema).json(json)
    }else{
      throw new RuntimeException("please provide file path and schema for readJsonFile")
    }
    df  
  }
  
  def orderDfByDeltaValue(df: DataFrame): DataFrame= {
    val sparkS = getSparkSession()
    import sparkS.implicits._
    
    var endOfDayPosDf = df    
    endOfDayPosDf = endOfDayPosDf.withColumn("DeltaAbsolute", when('Delta.lt(0), 'Delta * -1).otherwise('Delta))
    endOfDayPosDf = endOfDayPosDf.orderBy('DeltaAbsolute.desc, 'Instrument.desc).drop('DeltaAbsolute)
    endOfDayPosDf
  }
  
  def writeOutput(endOfDayPosDf: DataFrame, filePath: String){
    //write output as single file
    if(filePath!=null){
      endOfDayPosDf.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).option("delimiter", ",").save(filePath)
    }else{
      throw new RuntimeException("please provide file path for writeOutput")
    }
  } 

}
  case class StartOfDayPosCase(
    Instrument: Option[String],
    Account: Option[String],
    AccountType: Option[String],
    Quantity: Option[Long]
  )
  case class TxnCase(
    TransactionId: Option[Long],
    Instrument: Option[String],
    TransactionType: Option[String],
    TransactionQuantity: Option[Long]
  )