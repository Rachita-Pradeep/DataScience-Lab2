
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
// import java.io._

// import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation


//["series/cogtech/AlexanderssonP06", ["Jan Alexandersson", "Norbert Pfleger"], 2006],
class PaperRecord(paperString: String, authorString: String, yearString: String) extends Serializable {
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
    val authors: Array[String] = authorString.split(",").map( _.replace(" ", "")).filter(_.length > 0)
    val year: Int = yearString.toInt

    override def toString = s"$paper,$authors,$year"
    
}

object PaperRecord {
    def apply(line: String): Option[PaperRecord] = {
        val paperRecordRegex = "\\[(.*), \\[(.*)\\], (\\d{4})\\],".r
        line match {
            case paperRecordRegex(d, a, y) => {
                if(a.length > 0) Some(new PaperRecord(d, a, y))
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

  // def main(args: Array[String]) {

  // 	// val writer = new PrintWriter(new File(args(0)))
  // 	// def writeln(str: String): Unit = writer.write(str + '\n')

    // val conf = new SparkConf()
    //   .setAppName("dblpGraph")
    //   // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //   // .set("spark.kryo.registrator", "Registrator")
    // val sc = new SparkContext(conf)

  //   val paperFile = sc.textFile(args(0))

  //   val firstHundredLines = paperFile.take(100)

  //   // firstHundredLines.foreach(line => {

  //   //     val record = PaperRecord(line)
  //   //     record match {
  //   //         case Some(record) => println(record)
  //   //         case None => {}
  //   //     }


  //   // })

  //   val filteredRecords:Array[PaperRecord] = firstHundredLines
  //       // .map(line => {
  //       //     val record = PaperRecord(line)
  //       //     record match {
  //       //         case Some(record) => record
  //       //         case _ => None
  //       //     }
  //       //     record
  //       // })
  //       .map(line => PaperRecord(line))
  //       .flatMap(line => line )

  //   filteredRecords.foreach( record => {
  //       println("**************************************************")
  //       println(record.paper)
  //       record.authors.foreach(println)
  //       println(record.year)
  //   })

  //   // (0 until 100).foreach(i => {
  //   //     println(i + ": " + firstHundredLines(i))
  //   //     new PaperRecord(firstHundredLines(i))
  //   // })

    





  // }
}