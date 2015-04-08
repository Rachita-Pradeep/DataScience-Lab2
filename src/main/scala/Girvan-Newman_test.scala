package edu.cornell.tech.cs5304.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import scala.reflect.{ClassTag, classTag}

import org.apache.log4j.Logger
import org.apache.log4j.Level

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

	    // Logger.getLogger("org").setLevel(Level.WARN)
    	// Logger.getLogger("akka").setLevel(Level.WARN)

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
			//only for connected graphs?
			val gnGraph = GirvanNewman
				.computeBetweennessGraph(filteredGraph)
			gnGraph.cache()

			filteredGraph.unpersistVertices(blocking=false)
			filteredGraph.edges.unpersist(blocking=false)
			
			val sortedEdges = 
				gnGraph
				.triplets
				.sortBy(triplet => triplet.attr, false)

			// val edgeArray = sortedEdges
			// .collect
		
			// pw.write("\n***********************************\n\n" + i + "\n\n")
			// println("\n***********************************\n\n" + i + "\n\n")
			// edgeArray
			// .filter(triplet => triplet.srcId < triplet.dstId)
			// .foreach(triplet => {
			// 		pw.write( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId + "\n")
			// 		println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
			// 	})

			topEdge = sortedEdges.first

			sortedEdges.unpersist(blocking=false)

			//filter out any edges within .001% of the topEdge
			val difference = .99999
			filteredGraph = gnGraph
				.subgraph(epred = (et) => {
					et.attr < difference*topEdge.attr
				})

			gnGraph.unpersistVertices(blocking=false)
			gnGraph.edges.unpersist(blocking=false)

			i+=1


			//compute modularity



			val pair = GirvanNewman.computeModularity(filteredGraph)
			val numberOfComponents = pair._1
			val modularity = pair._2
			pw.write("\n***********************************\n" + i + "\n")
			// println("\n***********************************\n\n" + i + "\n\n")
			pw.write( "Modularity = " + modularity + ", numberOfComponents = " + numberOfComponents + "\n")
			// println( "Modularity = " + modularity + ", numberOfComponents = " + numberOfComponents)



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