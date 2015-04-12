
package edu.cornell.tech.cs5304.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// import org.apache.spark.rdd.RDD
// import org.apache.spark.mllib.linalg._
// import org.apache.spark.mllib.feature._

// import scala.math.sqrt
// import scala.math.pow
import java.io._

// import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation


//["series/cogtech/AlexanderssonP06", ["Jan Alexandersson", "Norbert Pfleger"], 2006],
class PaperRecord(paperString: String, authorString: String, yearString: String, val line: String) extends Serializable {
	// val paperRecordRegex = """[(.*), [(.*)], (\d{4})]""".r
 //    line match {
 //        case paperRecordRegex(paper, authorList, year) => println((paper, authorList, year))
 //        case _ => println("Did not match")
 //    }

    // val paperRecordRegex = "\\[(.*), \\[(.*)\\], (\\d{4})\\],".r
    // val triple = line match {
    //     case paperRecordRegex(d, a, y) => (d, a, y)
    //     case _ => None
    // }

    val paper: String = paperString.replace("\"", "")
    val paperSplit: Array[String] = paper.split("/")
    val series: String = paperSplit(0)
    val group: String = paperSplit(1)
    // val paperId: String = paperSplit(2)
    val authors: Array[String] = authorString.split(",").map( _.replace(" ", "").replace("\"", "")).filter(_.length > 0)
    //val authors: Array[String] = authorString.split(",").filter(_.length > 0)
    val year: Int = yearString.toInt

    override def toString = s"$paper,$authors,$year"
    
}

object PaperRecord {
    def apply(line: String): Option[PaperRecord] = {
        val paperRecordRegex = "\\[(.*), \\[(.*)\\], (\\d{4})\\],".r
        line match {
            case paperRecordRegex(d, a, y) => {
                if(a.length > 0) Some(new PaperRecord(d, a, y, line))
                else None
            }
            case _ => None
        }
    }
}
 
// import org.apache.hadoop.mapred.SequenceFileInputFormat
// import org.apache.hadoop.io.{LongWritable, Text}
 
// import com.esotericsoftware.kryo.Kryo
// import org.apache.spark.serializer.KryoRegistrator
 
// class Registrator extends KryoRegistrator {
//   override def registerClasses(kryo: Kryo) {
//     kryo.register(classOf[LongWritable])
//     kryo.register(classOf[Text])
//   }
// }

object DBLPGraph {
  /* find ngrams that match a regex; args are regex output input [input ..] */

  def main(args: Array[String]) {

  	// val writer = new PrintWriter(new File(args(0)))
  	// def writeln(str: String): Unit = writer.write(str + '\n')

    val conf = new SparkConf()
      .setAppName("dblpGraph")
      // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryo.registrator", "Registrator")
    val sc = new SparkContext(conf)

    val paperFile = sc.textFile(args(0))
    val outputDirectory = args(1)
    val filePrefix = args(2)


    // val firstHundredLines = paperFile

    // firstHundredLines.foreach(line => {

    //     val record = PaperRecord(line)
    //     record match {
    //         case Some(record) => println(record)
    //         case None => {}
    //     }


    // })

    

    val papers = paperFile
        // .map(line => {
        //     val record = PaperRecord(line)
        //     record match {
        //         case Some(record) => record
        //         case _ => None
        //     }
        //     record
        // })
        .map(line => PaperRecord(line))
        .flatMap(rec => rec )
        .filter(rec => rec.authors.length > 2)
        .filter( rec => if(args.length > 3) rec.paper.startsWith(args(3)) else true)

    val authors = 
        papers
        .flatMap(rec => rec.authors)
        .distinct


    // println("Number of papers: " + papers.count)
    // println("Number of authors: " + authors.count)

    val authorMap: Map[String, Long] = authors.zipWithIndex.collect.toMap

    val authorFile = outputDirectory + "dblp.authors." + filePrefix + ".csv"
    val aw = new java.io.PrintWriter(new File(authorFile))
    authorMap.foreach(pair => {

      aw.write(pair._1 + "," + pair._2 + "\n")

    })
    aw.close

    //for each paper
    //map authors to set of integers
    //compute pairwise combinations
    //output edges 0 1
    val edgeFile = outputDirectory + "dblp.edges." + filePrefix + ".txt"
    val ew = new java.io.PrintWriter(new File(edgeFile))
    val papersCollection = papers
    .flatMap(rec => {
      rec.authors
      .map(author => authorMap(author))
      .toList
      .combinations(2)
      .map(list => (list(0), list(1)))
    })
    .collect

    papersCollection
    .foreach(pair => ew.write(pair._1 + " " + pair._2 + "\n"))

    println("Number of nodes = " + authorMap.size)
    println("Number of edges = " + papersCollection.length)

    ew.close

    // authors.foreach(println)


    // groupedRecords.foreach(pair => {

    //   // val record = pair._2.head

    //   // val filteredRecords = pair._2.filter(rec => rec.authors.length >= 2).toArray.length
      


    //   // val uniqueAuthors = pair._2.flatMap(rec => rec.authors).toSet.size
    //   // println(pair._1 + " : " + uniqueAuthors + "nodes, "+ filteredRecords + " edges")



    //   // val outputFileName = outputDirectory + "dblp." + pair._1 + ".json"
    //   // val pw = new java.io.PrintWriter(new File(outputFileName))

    //   pair._2.foreach( rec => {

    //     println(rec.paper)
    //     // pw.write(rec.line + "\n")

    //   })

    //   // pw.close



    //   // val record = pair._2.head
    //   // println("**************************************************")
    //   // println(record.paper)
    //   // // println(record.series)
    //   // // println(record.group)
    //   // // println(record.paperId)
    //   // record.authors.foreach(println)
    //   // println(record.year)
    //   // println(record.line)

    // })


    // filteredRecords.foreach( record => {
    //     println("**************************************************")
    //     println(record.paper)
    //     println(record.series)
    //     println(record.group)
    //     println(record.paperId)
    //     record.authors.foreach(println)
    //     println(record.year)
    // })

    // (0 until 100).foreach(i => {
    //     println(i + ": " + firstHundredLines(i))
    //     new PaperRecord(firstHundredLines(i))
    // })

    //need to write files
    //need to create a graph where nodes are authors and edges exist if the authors have ever worked together
    //first, create map of authors to author ids


    





  }
}