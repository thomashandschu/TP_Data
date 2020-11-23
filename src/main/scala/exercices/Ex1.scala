package exercices

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Ex1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Q1
    val rdd = sparkSession.sparkContext.textFile("donnees.csv")

    //Q2
    val films_dc = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Il y a " + films_dc.count() + " films de di Caprio")

    //Q 3
    val notes_dc = films_dc.map(elem => (elem.split(";")(2).toDouble))
    val moyenne_notes = notes_dc.sum
    print("Moyenne des notes des films de di Caprio : " + moyenne_notes / films_dc.count() + "\n")

    //Q 4
    val vues_total_dc = films_dc.map(elem => (elem.split(";")(1).toDouble))
    val vues_total = rdd.map(elem => (elem.split(";")(1).toDouble))
    val poucentage_vues_DiCaprio = vues_total_dc.sum() / vues_total.sum()
    println("Pourcentage de vues des films de di Caprio : " + poucentage_vues_DiCaprio * 100 + " %")

    //Q 5
    val acteurs = rdd.map(item => (item.split(";")(3))).distinct.collect()
    acteurs.foreach(println)


  }

}
