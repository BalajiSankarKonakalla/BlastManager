package com.wissen

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object BlastManager extends Serializable {

  def main(args : Array[String]) {

    val inputPath = args(0)
    val intermediatePath = args(1)
    val outputPath = args(2)
    val numPartitions = args(3).toInt

    val spark = SparkSession
      .builder()
      .appName("blast")
      //.master("local[*]")
      .getOrCreate()

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    val fastaRdd = spark
      .sparkContext
      .textFile(inputPath)

    val modified =
    fastaRdd
      .zipWithIndex()
      .zipWithIndex()
      .map(x => (Math.floor( x._1._2 / 2 ), (x._1._1, x._2)) )
      .groupByKey()
      .map(x => x._2.toList)
      .filter(_.length == 2)
      .map(x => x.map(x => x._1))
      .map(x => x.head + "&" + x(1))


    val fastaRepartitionedRdd = modified.repartition(numPartitions)

    val fastaWithFormat = fastaRepartitionedRdd.map(x => x.replace("&", "\n"))

    val fastaWithPartition = fastaWithFormat.mapPartitionsWithIndex((index, x) => {
      x.map(s => s + "-" + index)
    })
     hdfs.mkdirs(new Path(outputPath))

    fastaWithPartition.foreachPartition(x => {
      import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

      import scala.sys.process._

      val index = x.take(1).toList.head.split("-").toList(1)
      val basePath = intermediatePath+"/"
      val fileName = basePath + "fastaFile-" + index + ".fasta"
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
      x.foreach( x => writer.write(x.split("-")(0) + "\n"))
      writer.close()

      val command = "cd /home/dc-user/blast/ncbi-blast-2.13.0/bin && blastn -query " + fileName + " -db GRCh38_latest_genomic.fna -outfmt '6 std scomname' -out " + basePath + "result-" + index + ".tab -subject_besthit"

      println(s"blastn -query $fileName -db GRCh38_latest_genomic.fna -outfmt 6 -out ${basePath}result-${index}.tab -subject_besthit")

      val res = Process(s"blastn -query $fileName -db GRCh38_latest_genomic.fna -max_hsps 1 -max_target_seqs 5 -outfmt 6 -out ${basePath}result-${index}.tab", new File("/home/dc-user/blast/ncbi-blast-2.13.0/bin")).!!

      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      hdfs.copyFromLocalFile(new Path(s"${basePath}result-${index}.tab"), new Path(outputPath + "/"))
    })
  }
}
