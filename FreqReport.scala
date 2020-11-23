import CONSTANTS.{argsPars, argsValid, normalize_value}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Encoders
import java.time._

object FreqReport {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession.builder.appName("Frequency Report").getOrCreate()
    import spark.implicits._

    val normalize_value_udf = spark.udf.register("normalize_value_udf",normalize_value)
    val optionsMap  = argsPars(args, "j") //Parse input arguments from command line
    val validMap    = argsValid(optionsMap) // Validation input arguments types and building Map

    val data_chain = spark.read.
      format("csv").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(validMap("path_data_chain").map(_.toString):_*)

    val data_position = spark.read.
      format("csv").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(validMap("path_data_position").map(_.toString):_*)

    val data_chainWork0 = data_chain.select(
      $"ClientID".cast(sql.types.StringType),
      $"user_path".cast(sql.types.StringType),
      $"timeline".cast(sql.types.StringType),
      $"target_numbers".cast(sql.types.StringType))

    val data_positionWork = data_position.select(
      $"channel_name".cast(sql.types.StringType),
      $"position".cast(sql.types.IntegerType),
      $"shapley_value".cast(sql.types.FloatType)
    )

    val windewSpec = Window.partitionBy($"ID").orderBy($"user_path")

    val data_chainWork1 = data_chainWork0.withColumn("row_number",row_number.over(windewSpec))

    val data_chainSeq = data_chainWork1.select(
      $"ClientID",
      split($"user_path","=>").as("channel_seq"),
      $"timeline",
      $"target_numbers",
      $"row_number"
    )

    val data_chainPos0 = data_chainSeq.select(
      $"ClientID",
      posexplode($"channel_seq"),
      $"row_number"
    )

    val data_chainPos1 = data_chainPos0.select(
      $"ClientID",
      $"col".as("channel_name"),
      $"pos".as("position"),
      $"row_number"
    )

    val data_chainValue = data_chainPos1.as("df1").
      join(data_positionWork.as("df2"),($"df1.ClientID" && $"df1.position"  === $"df2.ClientID" && $"df2.position"),"inner").
      select($"df1.*")

    val data_chainnValAgg = data_chainValue.groupBy($"ClientID",$"row_number").agg(collect_list($"shapley_value").as("shapley_value"))

    val data_chainNormalizeVal = data_chainnValAgg.select(
      $"ClientID",
      $"row_number",
      $"shapley_value",
      normalize_value_udf($"shapley_value").as("normalize_value")
    )

    val chain_result = data_chainWork1.as("df1").
      join(data_chainNormalizeVal.as("df2"),$"df1.ClientID" && $"df1.position"  === $"df2.ClientID" && $"df2.position","inner").
      select("df1.*")

    chain_result.coalesce(1).
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(validMap("output_path").head.toString)
















  }
}
