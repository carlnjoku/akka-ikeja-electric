package com.lightbend.akka.sample.routes

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.PathDirectives.pathEndOrSingleSlash
import akka.http.scaladsl.server.directives.RouteDirectives.complete

import org.apache.spark.sql.SparkSession
//import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._ // for `when`
/**
* Routes can be defined in separated classes like shown in here
*/
object BaseRoutes {

// This route is the one that listens to the top level '/'
lazy val baseRoutes: Route =
   pathEndOrSingleSlash { // Listens to the top `/`
      complete("Server up and running") // Completes with some text
     val spark = SparkSession.builder.
        master("local")
        .appName("spark session example")
        .getOrCreate()

         import spark.implicits._
         val df = spark.read.format("csv").option("header","true").csv("resources/input.csv")

         //df.printSchema()

        val input =  df.withColumn("sity_percent_tot_recv", round($"tot_recv"*0.6, 1))
        val input_1 =  input.withColumn("tot_con_by_unmetered", ($"tot_recv" - ($"md" +  $"prime" + $"tentv_ppm" + $"ppaid")) )

        val input_2 =  input_1.withColumn("actual_ppm", when($"tentv_ppm" > $"sity_percent_tot_recv", $"tentv_ppm"*0.6).otherwise($"tentv_ppm"))

        //.when($"tot_recv" > $"tentv_ppm", ($"tot_recv"*0.6))

        /*
         val input =  df.select($"dt_inp", $"TOT_RECV", $"MD", $"PRIME", $"TENTV_PPM", $"PPAID", ($"TOT_RECV" - ($"MD" +  $"PRIME" + $"TENTV_PPM" + $"PPAID")) .as("tot_con_by_unmetered") )
         //input.show()

         val input_1 = input.select($"dt_inp", ($"tot_recv").cast("int"), $"md", $"prime", ($"tentv_ppm").cast("int"), $"ppaid", $"tot_con_by_unmetered",  (($"TOT_RECV")*0.6) .as("SIXTY_PERCENT_TOT_RECV").cast("int"), (($"TENTV_PPM")*0.6) .as("SIXTY_PERCENT_TENTV_PPM").cast("int") )
         //input_1.show()
        */


         ////////////////////////////////////////////////////////////
         val population = spark.read.format("csv").option("header","true").csv("resources/population.csv")
         val population_1 = population.select($"dt_pop",    $"tclass_pop", $"month_1_pop",    $"month_2_pop",    $"month_3_pop",    $"month_4_pop",    $"month_5_pop",    $"month_6_pop", $"current_month_unmetered_pop",    ($"month_1_pop" +    $"month_2_pop" +    $"month_3_pop" +    $"month_4_pop" +    $"month_5_pop" + $"month_6_pop") .as("six_month_sum"), (($"month_1_pop" +    $"month_2_pop" +    $"month_3_pop" +    $"month_4_pop" +    $"month_5_pop" +    $"month_6_pop")/6) .as("six_month_avg_pop"), ($"current_month_unmetered_pop" + $"month_6_pop") .as("total_pop_meter_unm_current_month")    )

         //population_1.show()

         ////////////////////////////////////////////////////////////
         val energy_consumed = spark.read.format("csv").option("header","true").csv("resources/energy_consumed.csv")
         val energy_consumed_1 = energy_consumed.select(($"dt") .as("dt_con"), ($"tclass").as("tclass_con"), $"month_1",    $"month_2",    $"month_3",    $"month_4",    $"month_5",    $"month_6",  (($"month_1" +    $"month_2" +    $"month_3" + $"month_4" +    $"month_5" +    $"month_6")/6) .as("six_month_egy_avg"), ($"month_1" +    $"month_2" +    $"month_3" +    $"month_4" +    $"month_5" +    $"month_6") .as("six_month_egy_sum")    )

         //energy_consumed_1.show()


         val average = energy_consumed_1.join(population_1,  $"dt_con" === $"dt_pop" && $"tclass_con" === $"tclass_pop")


         val average_2 = average.select($"dt_con", $"tclass_con", ($"month_1"/$"month_1_pop") .as("month_1_avg").cast("int"), ($"month_2"/$"month_2_pop") .as("month_2_avg").cast("int"),
                                       ( $"month_3"/$"month_3_pop") .as("month_3_avg").cast("int"), ($"month_4"/$"month_4_pop") .as("month_4_avg").cast("int"), ($"month_5"/$"month_5_pop") .as("month_5_avg").cast("int"), ($"month_6"/$"month_6_pop") .as("month_6_avg").cast("int"))

         val average_3 = average_2.select(($"dt_con").as("dt_avg"), ($"tclass_con").as("tclass_avg"), (($"month_1_avg" + $"month_2_avg" + $"month_3_avg" + $"month_4_avg" + $"month_5_avg" + $"month_6_avg")/6 ).as("six_month_avg_con"))
         //average_3.show()

          /////////////////////////////////////////////////////////////////////////////////////
          // Generate result sheet
          // Use energy_consumed DF to generate the initial col of dt, undertaking and tariff class


          val result = energy_consumed_1.select(($"dt_con").as("dt_res"), ($"tclass_con").as("tclass_res"))

          // Join current month population and cunsumption to result
          val result_1 = result.join(energy_consumed_1, $"dt_res" === $"dt_con" && $"tclass_res" === $"tclass_con" )
                               .join(population_1, $"dt_res" === $"dt_pop" && $"tclass_res" === $"tclass_pop" )
                               .join(average_3, $"dt_res" === $"dt_avg" && $"tclass_res" === $"tclass_avg")


          val result_2 = result_1.select($"dt_res", $"tclass_res", $"month_6_pop", ($"month_6").as("current_month_con"), ($"month_6_pop").as("current_month_pop"), ($"month_6"/$"month_6_pop").as("current_month_metered_avg_con").cast("int"), $"six_month_avg_con", $"total_pop_meter_unm_current_month", (($"month_6_pop")*($"six_month_avg_con")).as("total_avg_metered_con") )

          //result_2.show()

          // Join result_2 and input_1 to get total comsumption by unmetered customers
          val result_3 = result_2.join(input_2, $"dt_res" === $"dt_inp")


          val result_4 = result_3.select($"dt_res", $"tclass_res", $"month_6_pop", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con".cast("int"), $"six_month_avg_con", $"total_avg_metered_con", $"total_pop_meter_unm_current_month", $"tot_con_by_unmetered")


          val group_total_average_metered_consumption = result_4.groupBy("dt_res").sum("total_avg_metered_con")


          val group_total_average_metered_consumption_1 = group_total_average_metered_consumption.select(($"dt_res").as("dt_grp"), $"sum(total_avg_metered_con)" )
          //group_total_average_metered_consumption_1.show()
          val result_6 = result_4.join(group_total_average_metered_consumption_1, $"dt_res" === $"dt_grp")

          //val result_7 = result_6.select($"dt_res", $"tclass_res", $"current_month_con", $"current_month_avg_con", ($"sum(total_avg_metered_con)")).as("total_avg_metered_con").orderBy($"dt_res".asc).show(100)

          val result_final = result_6.select($"dt_res",  $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con".cast("int"), $"six_month_avg_con", $"total_avg_metered_con", ($"tot_con_by_unmetered").cast("int"), $"sum(total_avg_metered_con)", ($"total_avg_metered_con"/$"sum(total_avg_metered_con)").as("weighted_avg"), $"total_pop_meter_unm_current_month", ($"total_pop_meter_unm_current_month" - $"current_month_pop").as("unmetered_cust_pop") )
          val result_final_1 = result_final.select($"dt_res",  $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con".cast("int"),
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"sum(total_avg_metered_con)", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"tot_con_by_unmetered", $"unmetered_cust_pop", $"weighted_avg",
                                                  ($"weighted_avg" * $"tot_con_by_unmetered"/$"unmetered_cust_pop").as("consumption_per_unmetered_cust"),
                                                  (($"weighted_avg" * $"tot_con_by_unmetered"/$"unmetered_cust_pop")*($"unmetered_cust_pop")).as("total_consump_by_unmetered_cust_by_class_by_dt")  )

          val result_final_2 = result_final_1.select($"dt_res",  $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"sum(total_avg_metered_con)", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"tot_con_by_unmetered", $"unmetered_cust_pop", $"weighted_avg", $"consumption_per_unmetered_cust", $"total_consump_by_unmetered_cust_by_class_by_dt", ($"consumption_per_unmetered_cust"*1).as("30_cap_recommd_unit_for_unmetered"), ($"consumption_per_unmetered_cust" - $"consumption_per_unmetered_cust").as("unbilled_eng_per_cust")  )

          val result_final_3 = result_final_2.select($"dt_res", $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"sum(total_avg_metered_con)", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"tot_con_by_unmetered", $"unmetered_cust_pop", $"weighted_avg", $"consumption_per_unmetered_cust", $"total_consump_by_unmetered_cust_by_class_by_dt", $"30_cap_recommd_unit_for_unmetered", $"unbilled_eng_per_cust", (($"30_cap_recommd_unit_for_unmetered")* $"unmetered_cust_pop").as("total_recommd_consumption_by_unmetered_cust"), ($"unbilled_eng_per_cust"* $"unmetered_cust_pop").as("total_unbilled_eng")  )

          val result_final_4 = result_final_3.select($"dt_res", $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"consumption_per_unmetered_cust", $"total_consump_by_unmetered_cust_by_class_by_dt", $"30_cap_recommd_unit_for_unmetered", $"unbilled_eng_per_cust", $"total_recommd_consumption_by_unmetered_cust", $"total_unbilled_eng" )


          val result_final_5 = result_final_4.na.fill(0)

          val prop=new java.util.Properties()
          prop.put("user","root")
          prop.put("password","")
          val url="jdbc:mysql://localhost:3306/ikeja"
          //df is a dataframe contains the data which you want to write.
          result_final_5.write.mode(SaveMode.Append).jdbc(url,"result",prop)



        /*
        1  $"current_month_pop" population_1
        2  $"current_month_con", energy_consumption_1
        3  $"current_month_metered_avg_con"  2/1
        4  $"six_month_avg_pop" population_1
        5  $"six_month_avg_con"  average_3
        6  $"total_metered_consumption_in_current_month" 1 X 2
        7  $"total_avg_con_by_class_by_dt" 1 X 5 total_avg_metered_con
        8  $"sum(total_avg_metered_con)" group_total_average_metered_consumption_1
        9  $"weighted_avg" = (7/8 )x 100
        10 $"total_customer_pop_by_dt" = current_month_unmetered_pop + month_6_pop
        11 $"tot_con_by_unmetered" tot_unmetered_con_1
        13 $"unmetered_cust_pop_by_dt" = 10 - 1
        14 $"consumption_by_unmetered_cust_by_dt"  = 9 *(11/13)
        15 $"tot_consumption_by_unmetered_cust_by_tariff_by_dt" 13*14


          //val fType = f._2
          //if (fType  == "StringType") { println(s"STRING_TYPE") }
          //if (fType  == "MapType") { println(s"MAP_TYPE") }
          //else {println("....")}
          //println("Name %s Type:%s - all:%s".format(fName , fType, f))

          //}


         //input_1.createOrReplaceTempView("input")
         //val df3 = spark.sql("Select * from input")
         */

         /*
         val selectedData = average.select("*")
         selectedData.write.format("csv").option("header", "true")
         .save(s"resources/test.csv")
         */



         /*
         // Load training data
          val training = spark.read.format("libsvm")
            .load("resources/sample_linear_regression_data.txt")

            training.show()


          val lr = new LinearRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)

          // Fit the model
          val lrModel = lr.fit(training)

          // Print the coefficients and intercept for linear regression
          println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

          // Summarize the model over the training set and print out some metrics
          val trainingSummary = lrModel.summary
          println(s"numIterations: ${trainingSummary.totalIterations}")
          println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
          trainingSummary.residuals.show()
          println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
          println(s"r2: ${trainingSummary.r2}")

         val prop=new java.util.Properties()
         prop.put("user","root")
         prop.put("password","")
         val url="jdbc:mysql://localhost:3306/ikeja"
         //df is a dataframe contains the data which you want to write.
         //res.write.mode(SaveMode.Append).jdbc(url,"data_grid",prop)
        */

        complete("Server up and running") // Completes with some text
   }
}
