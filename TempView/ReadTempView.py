from TempView.CreateTempView import spark
spark.sql("select * from `tempview`").show()