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

	    Logger.getLogger("org").setLevel(Level.WARN)
    	Logger.getLogger("akka").setLevel(Level.WARN)

		//graph file is in args(0)
		val inputGraph = GraphLoader.edgeListFile(sc, args(0))


		// inputGraph.triplets.foreach( triplet => {

		// 		println(triplet.srcId + " -> " + triplet.dstId)

		// 	}
		// )

		// println("edges in file: " + inputGraph.triplets.count)	

		val uniqueInputGraph = inputGraph.groupEdges( (e1, e2) => e1)

		// println("unique edges in file: " + uniqueInputGraph.triplets.count)	



		println("****************************************************")
		//graph file is undirected, but graphs are treated as directed
		//need to get edges, get reverse of edges, union the two and create a new graph

		val testGraph = Graph(uniqueInputGraph.vertices, inputGraph.edges ++ inputGraph.edges.reverse)

		//might need to group edges to remove duplicates

		// testGraph.triplets.foreach( triplet => {

		// 		println(triplet.srcId + " -> " + triplet.dstId)

		// 	}
		// )


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
		var numberOfGraphs = 10
		var removedEdgeSet: Set[EdgeTriplet[Int,Double]] = Set()
		while(!filteredGraph.edges.isEmpty && numberOfGraphs > 0) {

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
			// if(topEdge.srcId > topEdge.dstId) topEdge = topEdge.reverse

			sortedEdges.unpersist(blocking=false)

			//filter out any edges within .1% of the topEdge
			val difference = .999
			filteredGraph = gnGraph
				.subgraph(epred = (et) => {
					et.attr < difference*topEdge.attr
				})

			//compute modularity

			val pair = GirvanNewman.computeModularity(filteredGraph)
			val numberOfComponents = pair._1
			val modularity = pair._2
			pw.write("\n***********************************\n" + i + "\n")
			println("\n***********************************\n\n" + i + "\n\n")
			pw.write( "Modularity = " + modularity + ", numberOfComponents = " + numberOfComponents + "\n")
			println( "Modularity = " + modularity + ", numberOfComponents = " + numberOfComponents)

			if (numberOfComponents >= 2 && modularity < lastModularity)
			{

				
				//get connected components
				val cc = gnGraph
					.subgraph(epred = (et) => (et.srcId < et.dstId))
					.connectedComponents()

				println("Finished connected components")
				//map removed edges from current vertexId to connected vertexId
				// 1) extract vertexId from removed edges (src, dst)
				// 2) filter vertices in graph based on vertexId match
				// 3) create map old vertexId -> new vertexId
				// 4) map removed edges from old vertexId -> new vertexId
				// 5) filter edges srcId != dstId
				// 6) aggregate equivalent edges
				// 7) map edge values to relative value

				val vertexIdSet = removedEdgeSet.flatMap(triplet => Set(triplet.srcId, triplet.dstId))

				val vertexIdMap = 
					cc.vertices
					.filter(pair => vertexIdSet.contains(pair._1))
					.collectAsMap

				val ccRemovedEdgeList = 
					removedEdgeSet.toList
					.map(triplet => (vertexIdMap(triplet.srcId), vertexIdMap(triplet.dstId)))
					.filter(pair => pair._1 != pair._2)

				val finalEdgeList:List[((VertexId, VertexId), Double)] = 
					ccRemovedEdgeList
					.groupBy(pair => pair)
					.mapValues(list => list.length.toDouble/ccRemovedEdgeList.length.toDouble)
					.toList

				//get unique vertexIds
				// 1) map vertices to RDD[VertexId] containing connected component eq class rep
				// 2) count by value Map[VertexId, Long]
				// 3) map to relative values
				val vertexMapping = 
					cc.vertices
					.map(pair => pair._2)
					.countByValue
					.toMap

				val totalValue = vertexMapping.values.reduce(_ + _).toDouble
				val vertexMappingRelative: Map[VertexId, Double] = 
					vertexMapping.mapValues(cnt => cnt.toDouble / totalValue)


				//get largest connected component graph and continue that
				// 1) filter cc graph by largest vertexId via subgraph
				// 2) do inner joins on both vertex and edge RDD 
				// 3) set filter graph to new graph (including reverse edges)

				val largestCCVertexId = 
					vertexMapping.toList
					.sortBy(pair => pair._2)
					.reverse
					.head._1

				val filteredCCgraph = 
					cc.subgraph(vpred = (vid, attr) => (attr == largestCCVertexId))

				val filteredVertices = 
					gnGraph.vertices.innerJoin(filteredCCgraph.vertices)((vid, oldAttr, newAttr) => oldAttr)

				val filteredEdges = 
					gnGraph.edges.innerJoin(filteredCCgraph.edges)((vid1, vid2, oldAttr, newAttr) => oldAttr)


				filteredGraph = Graph(filteredVertices, filteredEdges ++ filteredEdges.reverse)

				// {
				// 	"id" : 0,
				// 	"nodes" : [
				// 		{ "id": 0,
				// 			"value" : 0.5,
				// 			"top_author" : "Turing" },
				// 		{ "id": 3,
				// 			"value" : 0.3,
				// 			"top_author" : "Hawking" },
				// 		{ "id": 8,
				// 			"value" : 0.2,
				// 			"top_author" : "Einstein" },
				// 	],
				// 	"edges" : [
				// 		{ 	"src_id" : 0,
				// 			"dst_id" : 3,
				// 			"value" : 0.7 },
				// 		{ 	"src_id" : 3,
				// 			"dst_id" : 8,
				// 			"value" : 0.2 }
				// 	]
				// },
				println("Biggest graph is " + largestCCVertexId + " with " + filteredGraph.edges.count + " edges.")
				pw.write("\n***********************************\n" + i + "\n")
				pw.write("{\n\"id\" : " + i + ",\n\"nodes\" : [\n")
				val verticesString: String = vertexMappingRelative.toList.foldLeft("")((str, pair) => str + "{\"id\": " + pair._1 + ", \"value\" : " + pair._2 + " }\n")
				// .foreach(pair => pw.write("{\"id\": " + pair._1 + ", \"value\" : " + pair._2) + " }\n")
				pw.write(verticesString)
				pw.write("],\n\"edges\" : [\n")
				val edgesString = finalEdgeList.foldLeft("") ((str, pair) => str + "{\"src_id\": " + pair._1._1 + ", \"dst_id\": " + pair._1._2 + ", \"value\" : " + pair._2 + " }\n")
				// finalEdgeList.foreach(pair => pw.write("{\"src_id\": " + pair._1._1 + ", \"dst_id\": " + pair._1._2 + ", \"value\" : " + pair._2 + " }\n")
				pw.write(edgesString)
				pw.write("],\n}\n")

				lastModularity = -1.0
				removedEdgeSet = Set()

				pw.write("\n***********************************\n" + i + "\n")
				pw.write("Biggest graph is " + largestCCVertexId + " with " + filteredGraph.edges.count + " edges.")
			}
			else 
			{
				lastModularity = modularity
				removedEdgeSet = removedEdgeSet + topEdge
			}


			gnGraph.unpersistVertices(blocking=false)
			gnGraph.edges.unpersist(blocking=false)

			i+=1

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