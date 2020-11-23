package exercices


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Ex2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val toDouble = udf[Double, String](_.toDouble)

    val spark = SparkSession.builder().master("local").getOrCreate()
    //Q1
    val df= spark.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("donnees.csv")
    //Q2

    val columnsList = List("nom_film", "nombre_vues", "note_film", "acteur_principal")

    val dfWithRightColumnNames = df.toDF(columnsList:_*)
    dfWithRightColumnNames.show()
    val df1 = dfWithRightColumnNames.withColumn("nombre_vues", toDouble(dfWithRightColumnNames("nombre_vues")))

    //Q3-2
    val films_DC = dfWithRightColumnNames.filter(col("acteur_principal")==="Di Caprio")
    print("Il y a " +films_DC.count()+ " films de Leonardo di Caprio. \n")

    //Q3-3
    val moyenne_notes_DC=dfWithRightColumnNames.filter(col("acteur_principal")==="Di Caprio").groupBy("acteur_principal").avg("note_film")
    moyenne_notes_DC.show

    //Q3-4
    val total_vues = dfWithRightColumnNames.agg(sum("nombre_vues").cast("double")).first.getDouble(0)

    val total_vues_dc =films_DC.agg(sum("nombre_vues").cast("double")).first.getDouble(0)

    val pourcentage_vues_ldc = (total_vues_dc / total_vues) * 100

    println("Pourcentage des vues de di Caprio : " +pourcentage_vues_ldc+" %")

    //Q3-5
    val moyenne_notes=dfWithRightColumnNames.groupBy("acteur_principal").mean("note_film")
    moyenne_notes.show

    //Q4
    val moyennes_vues = dfWithRightColumnNames.withColumn("pourcentage de vues", col("nombre_vues")/ total_vues * 100)
    moyennes_vues.show
  }
}
