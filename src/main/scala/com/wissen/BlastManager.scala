package com.wissen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object BlastManager {

  def main(args : Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("blast")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val fastaRdd = spark
      .sparkContext
      .textFile("/Users/balaji/Downloads/bio-info/10_ERR187285_1.fasta")

    val modified =           // Needed only if we can't modify the fasta creation
    fastaRdd
      .zipWithIndex()
      .zipWithIndex()
      .map(x => (Math.floor( x._1._2 / 3 ), (x._1._1, x._2)) )
      .groupByKey()
      .map(x => x._2.toList)
      .filter(_.length == 3)
      .map(x => x.map(x => x._1))
      .map(x => x(0) + "&" + x(1) + x(2))


    val fastaRepartitionedRdd = modified.repartition(2)

    val fastaWithFormat = fastaRepartitionedRdd.map(x => x.replace("&", "\n"))

    val fastaWithPartition = fastaWithFormat.mapPartitionsWithIndex((index, x) => {
      x.map(s => s + "-" + index)
    })


    fastaWithPartition.foreachPartition(x => {
      import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
      import scala.sys.process._

      val index = x.take(1).toList.head.split("-").toList(1)
      val basePath = "/Users/balaji/Downloads/bio-info/tmp/partitioned-inputs/"
      val fileName = basePath + "fastaFile-" + index + ".fasta"
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
      x.foreach( x => writer.write(x.split("-")(0) + "\n"))
      writer.close()

      val command = "cd /Users/balaji/Downloads/bio-info && /Users/balaji/Downloads/bio-info/ncbi-blast-2.13.0+/bin/blastn -query " + fileName + " -db GRCh38_latest_genomic.fna -outfmt 5 -out " + basePath + "result-" + index + ".xml"
      println(command)

      val res = Process(command)!!

      println(res)

    })

  }
}
