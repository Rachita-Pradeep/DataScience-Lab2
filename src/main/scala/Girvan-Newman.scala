package edu.cornell.tech.cs5304.lab2


import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths._
import scala.reflect.{ClassTag, classTag}

object GirvanNewman {

	//this mak contains the values of vertex 'contributors', including possibly the vertex itself
	type GNMap = Map[VertexId, Double]
	type RootGNMap = Map[VertexId, GNMap]
	private def makeMap(x: (VertexId, Double)*) = Map(x: _*)
	private def makeRootMap(x: (VertexId, GNMap)*) = Map(x: _*)

	private def addMaps(gnmap1: GNMap, gnmap2: GNMap): GNMap =
	    (gnmap1.keySet ++ gnmap2.keySet).map {
	      k => k -> (gnmap1.getOrElse(k, 0.0) + gnmap2.getOrElse(k, 0.0))
	    }.toMap

	private def replaceValuesInMap(gnmap1: GNMap, gnmap2: GNMap): GNMap =
	    (gnmap1.keySet ++ gnmap2.keySet).map {
	      k => k -> gnmap2.getOrElse(k, gnmap1(k))
	    }.toMap

	private def mergeMapsWithAdd(rootMap1: RootGNMap, rootMap2: RootGNMap): RootGNMap =
	    (rootMap1.keySet ++ rootMap2.keySet).map {
	      k => k -> addMaps(rootMap1.getOrElse(k, Map()), rootMap2.getOrElse(k, Map()))
	    }.toMap

	private def mergeMapsWithReplace(rootMap1: RootGNMap, rootMap2: RootGNMap): RootGNMap =
	    (rootMap1.keySet ++ rootMap2.keySet).map {
	      k => k -> replaceValuesInMap(rootMap1.getOrElse(k, rootMap2(k)), rootMap2.getOrElse(k, Map()))
	    }.toMap

	private def sumGNMap(gnmap: GNMap): Double = gnmap.values.foldLeft(0.0)(_ + _)


	def computeBetweennessGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxGroupSize: Int = Int.MaxValue): Graph[VD, Double] = {
	// def runAlternate[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {

		graph.cache()

		def betweennessGraphForRoots(roots:Seq[VertexId]): Graph[VD, Double] = {

			// each node contains a map, 
			// keys are landmarks, 
			// values are shortest path FROM node TO the root
			// note that since our graph is expected to be undirected,
			// source and destination should not matter
			roots.foreach(println)
			val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, roots)


			val terminalVerticesMap = collection.mutable.Map[VertexId, Set[VertexId]]()
			val noEdges = shortestPaths.vertices.context.emptyRDD(classTag[Edge[VertexId]])
			var shortestPathGraph: Graph[SPMap, VertexId] = Graph(shortestPaths.vertices, noEdges)
			shortestPathGraph.cache()



			for (root <- roots) {
				//create graph of all the edges required for shortest paths from root to all other nodes
				//mark edges with id of root
				//this will allow us route messages later
				val shortestPathGraphForRoot: Graph[SPMap, VertexId] = shortestPaths
					.subgraph(epred = (triplet) => {
						val srcDistance = triplet.srcAttr.getOrElse(root, Int.MaxValue)
						val dstDistance = triplet.dstAttr.getOrElse(root, Int.MaxValue)
						if ( srcDistance == Int.MaxValue) false
						else (srcDistance+1 == dstDistance)
						// triplet.srcAttr(root)+1 == triplet.dstAttr(root)
					})
					.mapEdges(pair => root)

				val terminalVertices: Set[VertexId] = 
					shortestPathGraphForRoot.outerJoinVertices(shortestPathGraph.outDegrees) { (id, oldAttr, outDegOpt) =>
					  outDegOpt match {
					    case Some(outDeg) => outDeg
					    case None => 0 // No outDegree means zero outDegree
					  }
					}
					.vertices
					.filter(pair => pair._2 == 0)
					.keys
					.collect
					.toSet

				terminalVerticesMap(root) = terminalVertices

				shortestPathGraph = Graph(shortestPathGraph.vertices,
				shortestPathGraph.edges ++ shortestPathGraphForRoot.edges)

			}

			// vertices contain maps
			// keys are roots (parameter)
			// values are the number of shortest paths to this node from the key
			
			val initialGraph = shortestPathGraph.mapVertices { (id, attr) =>
		      if (roots.contains(id)) makeRootMap(id -> makeMap(id -> 1.0)) else makeRootMap()
		    }

		    val initialMessage = makeRootMap()

		    // remember that this graph is a multiset of combined shortest path graphs above
		    // The edge attributes denote the root vertexId that they belong to
			val numShortestPathsGraph: Graph[RootGNMap, VertexId] = initialGraph.pregel(initialMessage, Int.MaxValue, EdgeDirection.Out)(
			  (id, attr, msg) => {
			  	mergeMapsWithReplace(attr, msg)
			  }, // Vertex Program
			  triplet => {  // Send Message
			  	//compute src sum
			  	val srcSum = sumGNMap(triplet.srcAttr.getOrElse(triplet.attr, makeMap()))
			  	val dstGNMap = triplet.dstAttr.getOrElse(triplet.attr, makeMap())
			  	val dstExpectation = dstGNMap.getOrElse(triplet.srcId, 0.0)
			  	
			  	//check against dst expectation
			  	//if different, must have changed,
			  	//send message with updated value
			  	//should probably change this to an approximation
			    if (srcSum != dstExpectation) {
			    	// println(triplet.attr + ": " + triplet.srcId + " -(" + srcSum +")-> " + triplet.dstId)
			    	Iterator((triplet.dstId, makeRootMap(triplet.attr -> makeMap(triplet.srcId -> srcSum))))
			    } else {
			      	Iterator.empty
			    }
			  },
			  (a,b) => mergeMapsWithReplace(a, b) // Merge Message
			  )


			// val verticesArray: Array[(VertexId, RootGNMap)] = numShortestPathsGraph.vertices.collect

			// roots.foreach(root => {
			// 	println("\nShortest paths for root " + root)
			// 	verticesArray.foreach( pair => {
			// 		println(pair._1 + ": " + sumGNMap(pair._2.getOrElse(root, Map())))
			// 	})
			// })

			val numShortestPathsWithEdgeWeightsGraph:Graph[RootGNMap, (VertexId, Double)] = 
			numShortestPathsGraph
			.mapTriplets(  triplet => (triplet.attr, sumGNMap(triplet.srcAttr(triplet.attr)) / sumGNMap(triplet.dstAttr(triplet.attr))))

			//for each node, create a map
			val initialGraph2 = numShortestPathsWithEdgeWeightsGraph.mapVertices { (id, attr) =>
		      roots.map(root => makeRootMap(root -> makeMap(id -> 1.0)))
		      	.foldLeft(makeRootMap())( (acc, x) => mergeMapsWithReplace(acc, x))
		    }
		
			val betweennessVertexGraph = initialGraph2.pregel(initialMessage, 10, EdgeDirection.In)(
			  (id, attr, msg) => {
			  	mergeMapsWithReplace(attr, msg)
			  },
			  triplet => {  // Send Message

			  	val dstSum:Double = sumGNMap(triplet.dstAttr.getOrElse(triplet.attr._1, makeMap()))
			  	val edgeWeight:Double = triplet.attr._2
			  	val srcGNMap = triplet.srcAttr.getOrElse(triplet.attr._1, makeMap())
			  	val srcExpectation = srcGNMap.getOrElse(triplet.dstId, 0.0)

			  	//check against dst expectation
			  	//if different, must have changed,
			  	//send message with updated value
			  	//should probably change this to an approximation
			    if (dstSum*edgeWeight != srcExpectation) {
			    	// println(triplet.attr + ": " + triplet.dstId + " -(" + dstSum*edgeWeight +")-> " + triplet.srcId)
			    	Iterator((triplet.srcId, makeRootMap(triplet.attr._1 -> makeMap(triplet.dstId -> dstSum*edgeWeight))))
			    } else {
			      	Iterator.empty
			    }
			  },
			  (a,b) => mergeMapsWithReplace(a, b) // Merge Message
			  )

			val verticesArray: Array[(VertexId, RootGNMap)] = betweennessVertexGraph.vertices.collect

			// roots.foreach(root => {
			// 	println("\nShortest paths for root " + root)
			// 	verticesArray.foreach( pair => {
			// 		println(pair._1 + ": " + sumGNMap(pair._2.getOrElse(root, Map())))
			// 	})
			// })

			val betweennessGraphEdges = 
			betweennessVertexGraph
			.mapTriplets(triplet => triplet.attr._2 * sumGNMap(triplet.dstAttr(triplet.attr._1)))
			.edges

			Graph(graph.vertices, betweennessGraphEdges ++ betweennessGraphEdges.reverse)
			.groupEdges( (e1, e2) => e1 + e2)
		}

		var graphVertices = graph.vertices.collect.map(pair => pair._1).toSeq
		var returnGraph = graph.mapEdges(e => 0.0)

		while (!graphVertices.isEmpty) {
			
			val singleBetweennessGraph = betweennessGraphForRoots(graphVertices.take(maxGroupSize))
			
			returnGraph = Graph(returnGraph.vertices,
				returnGraph.edges ++ singleBetweennessGraph.edges)
				.groupEdges( (e1, e2) => e1 + e2)

			singleBetweennessGraph.unpersist()
			returnGraph.cache()

			graphVertices = graphVertices.drop(maxGroupSize)

		}
		returnGraph

		// 5-(23.79047619047619)-> 8
		// 0-(21.504761904761907)-> 3
		// 3-(17.75238095238095)-> 6
		// 7-(17.504761904761907)-> 9
		// 6-(17.038095238095238)-> 8
		// 0-(16.038095238095238)-> 4
		// 0-(14.771428571428569)-> 1
		// 0-(14.771428571428569)-> 2
		// 8-(14.752380952380953)-> 10
		// 4-(14.704761904761904)-> 7
		// 9-(13.41904761904762)-> 10
		// 3-(12.99047619047619)-> 7
		// 1-(12.37142857142857)-> 5
		// 2-(12.37142857142857)-> 5
		// 6-(12.219047619047618)-> 9
		// 1-(2.0)-> 2

		// betweennessGraphForRoots(graph.vertices.collect.map(pair => pair._1).toSeq)
		// 5-(23.79047619047619)-> 8
		// 0-(21.504761904761907)-> 3
		// 3-(17.75238095238095)-> 6
		// 7-(17.504761904761907)-> 9
		// 6-(17.038095238095238)-> 8
		// 0-(16.038095238095238)-> 4
		// 0-(14.771428571428569)-> 1
		// 0-(14.771428571428569)-> 2
		// 8-(14.752380952380953)-> 10
		// 4-(14.704761904761904)-> 7
		// 9-(13.41904761904762)-> 10
		// 3-(12.99047619047619)-> 7
		// 1-(12.37142857142857)-> 5
		// 2-(12.37142857142857)-> 5
		// 6-(12.219047619047618)-> 9
		// 1-(2.0)-> 2
		// betweennessGraphForRoots(Seq(0))

	}
}