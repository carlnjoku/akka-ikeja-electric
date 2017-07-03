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


          val result_4 = result_3.select($"inID", $"dt_res", $"tclass_res", $"month_6_pop", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con".cast("int"), $"six_month_avg_con", $"total_avg_metered_con", $"total_pop_meter_unm_current_month", $"tot_con_by_unmetered")


          val group_total_average_metered_consumption = result_4.groupBy("dt_res").sum("total_avg_metered_con")


          val group_total_average_metered_consumption_1 = group_total_average_metered_consumption.select(($"dt_res").as("dt_grp"), $"sum(total_avg_metered_con)" )
          //group_total_average_metered_consumption_1.show()
          val result_6 = result_4.join(group_total_average_metered_consumption_1, $"dt_res" === $"dt_grp")

          //val result_7 = result_6.select($"dt_res", $"tclass_res", $"current_month_con", $"current_month_avg_con", ($"sum(total_avg_metered_con)")).as("total_avg_metered_con").orderBy($"dt_res".asc).show(100)

          val result_final = result_6.select($"inID", $"dt_res",  $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con".cast("int"), $"six_month_avg_con", $"total_avg_metered_con", ($"tot_con_by_unmetered").cast("int"), $"sum(total_avg_metered_con)", ($"total_avg_metered_con"/$"sum(total_avg_metered_con)").as("weighted_avg"), $"total_pop_meter_unm_current_month", ($"total_pop_meter_unm_current_month" - $"current_month_pop").as("unmetered_cust_pop") )
          val result_final_1 = result_final.select($"inID", $"dt_res",  $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con".cast("int"),
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"sum(total_avg_metered_con)", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"tot_con_by_unmetered", $"unmetered_cust_pop", $"weighted_avg",
                                                  ($"weighted_avg" * $"tot_con_by_unmetered"/$"unmetered_cust_pop").as("consumption_per_unmetered_cust"),
                                                  (($"weighted_avg" * $"tot_con_by_unmetered"/$"unmetered_cust_pop")*($"unmetered_cust_pop")).as("total_consump_by_unmetered_cust_by_class_by_dt")  )

          val estimate = result_final_1.withColumn("recommeded_est", when($"consumption_per_unmetered_cust" > $"current_month_metered_avg_con", $"current_month_metered_avg_con"*1.30).otherwise($"consumption_per_unmetered_cust"))


          val result_final_2 = estimate.select($"inID", $"dt_res", $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con",  $"total_avg_metered_con", $"tot_con_by_unmetered", $"sum(total_avg_metered_con)", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"tot_con_by_unmetered", $"unmetered_cust_pop", $"consumption_per_unmetered_cust", $"recommeded_est", $"total_consump_by_unmetered_cust_by_class_by_dt", ($"consumption_per_unmetered_cust" - $"consumption_per_unmetered_cust").as("unbilled_eng_per_cust"))


          val result_final_3 = result_final_2.select($"inID", $"dt_res", $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"sum(total_avg_metered_con)", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"recommeded_est", $"unmetered_cust_pop", $"tot_con_by_unmetered", $"unmetered_cust_pop", $"weighted_avg", $"consumption_per_unmetered_cust",  $"total_consump_by_unmetered_cust_by_class_by_dt", $"unbilled_eng_per_cust", (($"recommeded_est")* $"unmetered_cust_pop").as("total_recommd_consumption_by_unmetered_cust"), ($"unbilled_eng_per_cust"* $"unmetered_cust_pop").as("total_unbilled_eng"))


          val result_final_4 = result_final_3.select($"inID", $"dt_res", $"tclass_res", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"consumption_per_unmetered_cust", $"total_consump_by_unmetered_cust_by_class_by_dt", $"recommeded_est", $"unbilled_eng_per_cust", $"total_recommd_consumption_by_unmetered_cust", $"total_unbilled_eng")


          //val est = result_final_4.select(round($"recommeded_est",2), $"current_month_metered_avg_con", $"consumption_per_unmetered_cust").show()

          val grid_data = spark.read.format("csv").option("header","true").csv("resources/data_grid_format.csv")
          val full_result = grid_data.join(result_final_4, $"dt_res" === $"dt_no" && $"tariff_class" === $"tclass_res")

          val full_result_1 = full_result.select($"inID", $"dt_no", $"tariff_class", $"undertaking", $"business_unit", $"current_month_con", $"current_month_pop", $"current_month_metered_avg_con",
                                                  $"six_month_avg_con", $"total_avg_metered_con", $"tot_con_by_unmetered", $"weighted_avg",
                                                  $"total_pop_meter_unm_current_month", $"unmetered_cust_pop", $"consumption_per_unmetered_cust", $"total_consump_by_unmetered_cust_by_class_by_dt", $"recommeded_est", $"unbilled_eng_per_cust", $"total_recommd_consumption_by_unmetered_cust", $"total_unbilled_eng")

          //val full_result_1 = result_final_4.na.fill(0)

          val prop=new java.util.Properties()
          prop.put("user","root")
          prop.put("password","")
          val url="jdbc:mysql://localhost:3306/ikeja_scala"
          //df is a dataframe contains the data which you want to write.
          full_result_1.write.mode(SaveMode.Append).jdbc(url,"result",prop)


          /*
          val selectedData = average.select("*")
          selectedData.write.format("csv").option("header", "true")
          .save(s"resources/test.csv")
          */

          val billing_eff_1 = input_2.select($"dt_inp", $"tot_recv", $"md", $"prime", $"actual_ppm", $"ppaid")
          val billing_eff_2 = result_final_4.groupBy("dt_res").sum("total_recommd_consumption_by_unmetered_cust")
          val billing_eff_3 = result_final_4.groupBy("dt_res").sum("total_unbilled_eng")


          val billing_eff_4 = billing_eff_1.join(billing_eff_2, $"dt_inp" === $"dt_res")
          val billing_eff_5 = billing_eff_4.select($"dt_inp", $"tot_recv", $"md", $"prime", $"actual_ppm", $"ppaid", $"sum(total_recommd_consumption_by_unmetered_cust)")

          val billing_eff_6 = billing_eff_5.join(billing_eff_3, $"dt_inp" === $"dt_res")

          val billing_eff_7 = billing_eff_6.select($"dt_inp", $"tot_recv", $"md", $"prime", $"actual_ppm", $"ppaid", $"sum(total_recommd_consumption_by_unmetered_cust)", $"sum(total_unbilled_eng)" )

          val billing_eff_8 = billing_eff_7.select($"dt_inp", $"tot_recv", $"md", $"prime", $"actual_ppm", $"ppaid", $"sum(total_recommd_consumption_by_unmetered_cust)", ($"md" + $"prime" + $"actual_ppm" + $"ppaid" + $"sum(total_recommd_consumption_by_unmetered_cust)").as("total_billed"), $"sum(total_unbilled_eng)" )

          val billing_eff_9 = billing_eff_8.select($"dt_inp", $"tot_recv", $"md", $"prime", $"actual_ppm", $"ppaid", ($"sum(total_recommd_consumption_by_unmetered_cust)").as("total_recommd_consumption_by_unmetered_cust"), $"total_billed", ($"total_billed"/$"tot_recv").as("billing_efficincy"), ($"sum(total_unbilled_eng)").as("total_unbilled_eng"), ($"tot_recv" - $"total_billed").as("energy_loss") )

          val billing_eff_final = billing_eff_9.select($"dt_inp", $"tot_recv", $"md", $"prime", $"actual_ppm", $"ppaid", $"total_recommd_consumption_by_unmetered_cust", $"total_billed", $"billing_efficincy", $"total_unbilled_eng", ($"tot_recv" - $"total_billed").as("energy_loss") )


          val prop1=new java.util.Properties()
          prop1.put("user","root")
          prop1.put("password","")
          val url_1="jdbc:mysql://localhost:3306/ikeja_scala"
          //df is a dataframe contains the data which you want to write.
          billing_eff_final.write.mode(SaveMode.Append).jdbc(url_1,"billing_efficiency",prop1)


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
