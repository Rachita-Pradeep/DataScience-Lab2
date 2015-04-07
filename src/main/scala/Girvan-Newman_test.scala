package edu.cornell.tech.cs5304.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import scala.reflect.{ClassTag, classTag}

import java.io._


object GirvanNewmanTest {
	def main(args: Array[String]) {

		println("Edges file = " + args(0))
		val outputFilename = args(1)
		
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


		// GirvanNewman.run(testGraph)
		// GirvanNewman.runAlternate(testGraph)
		val pw = new java.io.PrintWriter(new File(outputFilename))

		// var gnGraph = GirvanNewman
		// 	.computeBetweennessGraph(testGraph)
		// 	.subgraph(epred = (et) => et.srcId < et.dstId)

		// 	pw.write("\n***********************************\n\n")
		// 	println("\n***********************************\n\n")
		// 	val sortedEdges = 
		// 		gnGraph
		// 		.triplets
		// 		.sortBy(triplet => triplet.attr, false)

			// sortedEdges.collect.foreach(triplet => {
			// 		pw.write( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId + "\n")
			// 		println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
			// 	})

			// val topEdge = sortedEdges.first
			// val filteredGraph = gnGraph
			// 	.subgraph(epred = (et) => et != topEdge)

			// gnGraph = GirvanNewman
			// 	.computeBetweennessGraph(Graph(filteredGraph.vertices, filteredGraph.edges ++ filteredGraph.edges))
			// 	.subgraph(epred = (et) => et.srcId < et.dstId)

		var filteredGraph = testGraph.mapEdges(edge => 0.0)
		filteredGraph.cache()
		// var gnGraph: Graph[Int, Double] = filteredGraph
		var topEdge:EdgeTriplet[Int,Double] = null
		var lastModularity = -1.0

		var i=0
		while(!filteredGraph.edges.isEmpty) {

			// println()
			val gnGraph = GirvanNewman
				.computeBetweennessGraph(filteredGraph)
			gnGraph.cache()

			filteredGraph.unpersistVertices(blocking=false)
			filteredGraph.edges.unpersist(blocking=false)
			
			val sortedEdges = 
				gnGraph
				.triplets
				.sortBy(triplet => triplet.attr, false)

			val edgeArray = sortedEdges
			.collect
		
			pw.write("\n***********************************\n\n" + i + "\n\n")
			println("\n***********************************\n\n" + i + "\n\n")
			edgeArray
			.filter(triplet => triplet.srcId < triplet.dstId)
			.foreach(triplet => {
					pw.write( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId + "\n")
					println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
				})

			topEdge = sortedEdges.first

			sortedEdges.unpersist(blocking=false)

			filteredGraph = gnGraph
				.subgraph(epred = (et) => {
					!(((et.srcId == topEdge.srcId) && (et.dstId == topEdge.dstId)) ||
					((et.srcId == topEdge.dstId) && (et.dstId == topEdge.srcId)))
				})

			gnGraph.unpersistVertices(blocking=false)
			gnGraph.edges.unpersist(blocking=false)

			i+=1


			//compute modularity


			val modularity = GirvanNewman.computeModularity(filteredGraph)
			pw.write( "Modularity = " + modularity + "\n")
			println( "Modularity = " + modularity)



		}

		pw.close()

		// gnGraph
		// 	.triplets
		// 	.sortBy(triplet => triplet.attr, false)
		// 	.foreach(triplet => {
		// 		println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
		// 	})
			// .take(3)
			// .map(triplet => (triplet.srcId, triplet.dstId))
			// .toSet

		// gnGraphTop3Edges.foreach(pair => println(pair._1 + " -> " + pair._2))

		// val filteredGNGraph = gnGraph
		// 	.subgraph(epred = (et) => !gnGraphTop3Edges.contains(et.srcId, et.dstId))

		// filteredGNGraph.triplets.foreach(triplet => {
		// 	println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
		// })

		




	}
}