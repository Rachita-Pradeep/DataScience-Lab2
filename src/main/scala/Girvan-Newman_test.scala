package edu.cornell.tech.cs5304.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._


object GirvanNewmanTest {
	def main(args: Array[String]) {

		println("Edges file = " + args(0))
		
		val conf = new SparkConf()
	      .setAppName("GirvinNewmanTest")
	      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	      // .set("spark.kryo.registrator", "Registrator")
	    val sc = new SparkContext(conf)

		//graph file is in args(0)
		val inputGraph = GraphLoader.edgeListFile(sc, args(0))


		inputGraph.triplets.foreach( triplet => {

				println(triplet.srcId + " -> " + triplet.dstId)

			}
		)

		println("****************************************************")
		//graph file is undirected, but graphs are treated as directed
		//need to get edges, get reverse of edges, union the two and create a new graph

		val testGraph = Graph(inputGraph.vertices, inputGraph.edges ++ inputGraph.edges.reverse)

		//might need to group edges to remove duplicates

		testGraph.triplets.foreach( triplet => {

				println(triplet.srcId + " -> " + triplet.dstId)

			}
		)


		val GNGraph = GirvanNewman.run(testGraph).subgraph(epred = (et) => et.srcId < et.dstId)
		GNGraph.triplets.foreach(triplet => {
			println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
		})

		




	}
}