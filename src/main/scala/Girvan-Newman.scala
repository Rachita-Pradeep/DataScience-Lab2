package edu.cornell.tech.cs5304.lab2


import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths._
import scala.reflect.ClassTag

object GirvanNewman {

	def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, Double] = {

		graph.cache()

		// val landmarks: Seq[VertexId] = graph.vertices.collect.map(pair => pair._1).toSeq
		// val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, landmarks)

		

		def betweennessGraphForRoot(root:VertexId): Graph[VD, Double] = {
		// def betweennessGraphForRoot(root:VertexId): Unit = {

			val landmarks: Seq[VertexId] = Seq(root)

			val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, landmarks)

			val shortestPathsMap: SPMap = shortestPaths.vertices.collect.toMap
			.map(pair => (pair._1, pair._2(root)))
			// println("Shortest Path from " + root)
			// shortestPathsMap.foreach{ case (vid:VertexId, len:Int) => {
			// 		println(root + " -> " + vid + ": " + len)
			// 	}
			// }
			val groupedShortestPaths = shortestPathsMap.groupBy(pair => pair._2)
			val maxDistance = groupedShortestPaths.keys.max
			val furthestVerticesSet = groupedShortestPaths(maxDistance).keys.toSet

			// println("furthest vertices")
			// furthestVerticesSet.foreach(println)

			val shortestPathGraph = graph.subgraph(epred = (triplet) => (shortestPathsMap(triplet.srcId)+1 == shortestPathsMap(triplet.dstId)))

			val nonTerminalVertices: Set[VertexId] = shortestPathGraph.outDegrees
				.keys
				.collect
				.toSet

			val terminalVertices: Set[VertexId] = shortestPathGraph
				.vertices
				.filter(pair => !(nonTerminalVertices.contains(pair._1)))
				.keys
				.collect
				.toSet

			// println("terminal vertices")
			// terminalVertices.foreach(println)


			// shortestPathGraph.triplets.foreach(triplet => {
			// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
			// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
			// 		", " + triplet.dstAttr + ")")
			// })
			val initialGraph = shortestPathGraph.mapVertices((id, _) => if (id == root) 1.0 else Double.PositiveInfinity)
			val numShortestPathsGraph = initialGraph.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.Out)(
			  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
			  triplet => {  // Send Message
			    if (triplet.srcAttr != Double.PositiveInfinity) {
			    	// println(triplet.srcId + " -> " + triplet.dstId)
			    	// if(triplet.srcId == sourceId) Iterator((triplet.dstId, 1.0))
			     //  	else Iterator((triplet.dstId, triplet.srcAttr))
			     Iterator((triplet.dstId, triplet.srcAttr))
			    } else {
			      	Iterator.empty
			    }
			  },
			  (a,b) => a + b // Merge Message
			  )

			// numShortestPathsGraph.vertices.collect
			// .foreach { case (vid:VertexId, numPaths:Double) => {
			// 		println( vid + ": " + numPaths)
			// 	}
			// }

			val numShortestPathsWithEdgeWeightsGraph = 
			numShortestPathsGraph
			.mapTriplets(triplet => triplet.srcAttr / triplet.dstAttr)

			// // numShortestPathsWithEdgeWeightsGraph.triplets.foreach(triplet => {
			// // 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
			// // 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
			// // 		", " + triplet.dstAttr + ")")
			// // })


			//this needs to handle all nodes that don't have outlinks
			val initialGraph2 = numShortestPathsWithEdgeWeightsGraph
			// .mapVertices((id, _) => if (furthestVerticesSet.contains(id)) 1.0 else Double.PositiveInfinity)
			.mapVertices((id, _) => if (terminalVertices.contains(id)) 1.0 else Double.PositiveInfinity)
		
			val betweennessVertexGraph = initialGraph2.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.In)(
			  // (id, dist, newDist) => math.min(dist, 1.0 + newDist), // Vertex Program
			  (id, dist, newDist) => {
			  	if (terminalVertices.contains(id)) dist
			  	else if (dist == Double.PositiveInfinity) math.min(dist, 1.0 + newDist)
			  	else dist + newDist

			  },
			  triplet => {  // Send Message
			    if (triplet.dstAttr != Double.PositiveInfinity) {
			    	// println("\n\n\n\n\n" + triplet.srcId + " -> " + triplet.dstId)
			    	// if(triplet.srcId == sourceId) Iterator((triplet.dstId, 1.0))
			     //  	else Iterator((triplet.dstId, triplet.srcAttr))
			      	Iterator((triplet.srcId, triplet.dstAttr*triplet.attr))
			    } else {
			      	Iterator.empty
			    }
			  },
			  (a,b) => a + b // Merge Message
			  )

			// betweennessVertexGraph.triplets.foreach(triplet => {
			// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
			// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
			// 		", " + triplet.dstAttr + ")")
			// })

			val betweennessGraph = 
			betweennessVertexGraph
			.mapTriplets(triplet => triplet.attr * triplet.dstAttr)

			// Graph(graph.vertices, betweennessGraph.edges).triplets.foreach(triplet => {
			// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
			// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
			// 		", " + triplet.dstAttr + ")")
			// })

			Graph(graph.vertices, betweennessGraph.edges)
		}

		var returnGraph = graph.mapEdges(e => 0.0)
		returnGraph.cache()

		val vertexIterator : Iterator[VertexId] = graph.vertices.keys.toLocalIterator

		// val vertexIterator  = (0 to 1).iterator
		while(vertexIterator.hasNext) {

			val singleBetweennessGraph = 
				betweennessGraphForRoot(vertexIterator.next)

			// singleBetweennessGraph.triplets.foreach(triplet => {
			// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
			// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
			// 		", " + triplet.dstAttr + ")")
			// })

			returnGraph = Graph(returnGraph.vertices,
				returnGraph.edges ++ singleBetweennessGraph.edges ++ singleBetweennessGraph.edges.reverse)
				.groupEdges( (e1, e2) => e1 + e2)
		}

		// returnGraph.triplets.foreach(triplet => {
		// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
		// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
		// 		", " + triplet.dstAttr + ")")
		// })

		// returnGraph.triplets.foreach(triplet => {
		// 	println( triplet.srcId + "-(" + triplet.attr + ")-> " + triplet.dstId)
		// })

		returnGraph


		// val landmarks: Seq[VertexId] = graph.vertices.collect.map(pair => pair._1).toSeq
		// // // val landmarks: Seq[VertexId] = Seq(0)

		// val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, landmarks)

		// // Array[(VertexId, Map[VertexId, Int])]
		// val sourceId = 0

		// // println("Shortest Path from " + source_id)
		// // shortestPaths.vertices.collect
		// // .foreach{ case (vid:VertexId, pathMap:Map[VertexId, Int]) => {
		// // 		println(source_id + " -> " + vid + ": " + pathMap(source_id))
		// // 	}
		// // }

		// val shortestPathsMap: Map[VertexId, Map[VertexId, Int]] = shortestPaths.vertices.collect.toMap
		// // shortestPathsMap(sourceId).foreach { case (vid:VertexId, length:Int) => {
		// // 		println(sourceId + " -> " + vid + ": " + length)
		// // 	}
		// // }

		// val groupedShortestPaths = shortestPathsMap(sourceId)
		// 	.groupBy(pair => pair._2)

		// val maxDistance = groupedShortestPaths.keys.max

		// val furthestVerticesSet = groupedShortestPaths(maxDistance).keys.toSet

		// //for each vertex

		


		// //Init all vertices in group 1 = 1

		// //for each group, where g begins at g=2
		// //send messages from nodes in g-1 to g (connected), total

		// //to make this easier, we can construct a new graph of only edges where g-1 -> g, i.e., path length is increasing by 1

		// val sourceShortestPathMap = shortestPathsMap(sourceId)
		// val shortestPathGraph = graph.subgraph(epred = (triplet) => (sourceShortestPathMap(triplet.srcId)+1 == sourceShortestPathMap(triplet.dstId)))

		// //then we can use the pregel api to do the rest

		// //for each node in shortestPathGraph, compute number of shortest paths
		// //flowing from A -> K, each node is the sum of it's parents

		// val initialGraph = shortestPathGraph.mapVertices((id, _) => if (id == sourceId) 1.0 else Double.PositiveInfinity)
		// val numShortestPathsGraph = initialGraph.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.Out)(
		//   (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
		//   triplet => {  // Send Message
		//     if (triplet.srcAttr != Double.PositiveInfinity) {
		//     	// println(triplet.srcId + " -> " + triplet.dstId)
		//     	// if(triplet.srcId == sourceId) Iterator((triplet.dstId, 1.0))
		//      //  	else Iterator((triplet.dstId, triplet.srcAttr))
		//      Iterator((triplet.dstId, triplet.srcAttr))
		//     } else {
		//       	Iterator.empty
		//     }
		//   },
		//   (a,b) => a + b // Merge Message
		//   )

		// numShortestPathsGraph.vertices.collect
		// .foreach { case (vid:VertexId, numPaths:Int) => {
		// 		println( vid + ": " + numPaths)
		// 	}
		// }
		// //next, map each edge value to be the ratio of src.Attr / dst.Attr

		// val numShortestPathsWithEdgeWeightsGraph = 
		// numShortestPathsGraph.mapTriplets(triplet => triplet.srcAttr / triplet.dstAttr)


		// val initialGraph2 = numShortestPathsWithEdgeWeightsGraph
		// 	.mapVertices((id, _) => if (furthestVerticesSet.contains(id)) 1.0 else Double.PositiveInfinity)
		
		// val betweennessVertexGraph = initialGraph2.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.In)(
		//   (id, dist, newDist) => math.min(dist, 1.0 + newDist), // Vertex Program
		//   triplet => {  // Send Message
		//     if (triplet.dstAttr != Double.PositiveInfinity) {
		//     	// println("\n\n\n\n\n" + triplet.srcId + " -> " + triplet.dstId)
		//     	// if(triplet.srcId == sourceId) Iterator((triplet.dstId, 1.0))
		//      //  	else Iterator((triplet.dstId, triplet.srcAttr))
		//       	Iterator((triplet.srcId, triplet.dstAttr*triplet.attr))
		//     } else {
		//       	Iterator.empty
		//     }
		//   },
		//   (a,b) => a + b // Merge Message
		//   )

		// val betweennessGraph = 
		// betweennessVertexGraph.mapTriplets(triplet => triplet.attr * triplet.dstAttr)

		// // numShortestPathsWithEdgeWeightsGraph.triplets.foreach(triplet => {
		// // 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
		// // 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
		// // 		", " + triplet.dstAttr + ")")
		// // })
		// //finally, use pregel to set edge weights.
		// //node value = 1 + sum(child edges) (i.e., message values)


		// //new edge values will be current edge value + (node valueA * edge valueA)
		// //update both ways!!!

		// numShortestPathsGraph.vertices.collect
		// .foreach { case (vid:VertexId, numPaths:Double) => {
		// 		println( vid + ": " + numPaths)
		// 	}
		// }

		// numShortestPathsWithEdgeWeightsGraph.triplets.foreach(triplet => {
		// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
		// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
		// 		", " + triplet.dstAttr + ")")
		// })

		// // initialGraph2.vertices.collect
		// // .foreach { case (vid:VertexId, betweennessValue:Double) => {
		// // 		println( vid + ": " + betweennessValue)
		// // 	}
		// // }

		// betweennessVertexGraph.vertices.collect
		// .foreach { case (vid:VertexId, betweennessValue:Double) => {
		// 		println( vid + ": " + betweennessValue)
		// 	}
		// }

		// betweennessGraph.triplets.foreach(triplet => {
		// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
		// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
		// 		", " + triplet.dstAttr + ")")
		// })

		// val computedBetweennessGraph = betweennessGraphForRoot(0)

		// computedBetweennessGraph.triplets.foreach(triplet => {
		// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
		// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
		// 		", " + triplet.dstAttr + ")")
		// })

		// betweennessGraphForRoot(0)

		// val root = 0

		// val landmarks: Seq[VertexId] = Seq(root)

		// 	val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, landmarks)

		// 	val shortestPathsMap: Map[VertexId, Int] = shortestPaths.vertices.collect.toMap
		// 	.mapValues(m => m(root))
		// 	// println("Shortest Path from " + root)
		// 	// shortestPathsMap.foreach{ case (vid:VertexId, len:Int) => {
		// 	// 		println(root + " -> " + vid + ": " + len)
		// 	// 	}
		// 	// }
		// 	val groupedShortestPaths = shortestPathsMap.groupBy(pair => pair._2)
		// 	val maxDistance = groupedShortestPaths.keys.max
		// 	val furthestVerticesSet = groupedShortestPaths(maxDistance).keys.toSet

		// 	val shortestPathGraph = graph.subgraph(epred = (triplet) => (shortestPathsMap(triplet.srcId)+1 == shortestPathsMap(triplet.dstId)))

		// 	val initialGraph = shortestPathGraph.mapVertices((id, _) => if (id == root) 1.0 else Double.PositiveInfinity)
		// 	val numShortestPathsGraph = initialGraph.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.Out)(
		// 	  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
		// 	  triplet => {  // Send Message
		// 	    if (triplet.srcAttr != Double.PositiveInfinity) {
		// 	    	// println(triplet.srcId + " -> " + triplet.dstId)
		// 	    	// if(triplet.srcId == sourceId) Iterator((triplet.dstId, 1.0))
		// 	     //  	else Iterator((triplet.dstId, triplet.srcAttr))
		// 	     Iterator((triplet.dstId, triplet.srcAttr))
		// 	    } else {
		// 	      	Iterator.empty
		// 	    }
		// 	  },
		// 	  (a,b) => a + b // Merge Message
		// 	  )

		// 	val numShortestPathsWithEdgeWeightsGraph = 
		// 	numShortestPathsGraph
		// 	.mapTriplets(triplet => triplet.srcAttr / triplet.dstAttr)

		// 	val initialGraph2 = numShortestPathsWithEdgeWeightsGraph
		// 	.mapVertices((id, _) => if (furthestVerticesSet.contains(id)) 1.0 else Double.PositiveInfinity)
		
		// 	val betweennessVertexGraph = initialGraph2.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.In)(
		// 	  (id, dist, newDist) => math.min(dist, 1.0 + newDist), // Vertex Program
		// 	  triplet => {  // Send Message
		// 	    if (triplet.dstAttr != Double.PositiveInfinity) {
		// 	    	// println("\n\n\n\n\n" + triplet.srcId + " -> " + triplet.dstId)
		// 	    	// if(triplet.srcId == sourceId) Iterator((triplet.dstId, 1.0))
		// 	     //  	else Iterator((triplet.dstId, triplet.srcAttr))
		// 	      	Iterator((triplet.srcId, triplet.dstAttr*triplet.attr))
		// 	    } else {
		// 	      	Iterator.empty
		// 	    }
		// 	  },
		// 	  (a,b) => a + b // Merge Message
		// 	  )

		// 	val tempBetweennessGraph = 
		// 	betweennessVertexGraph
		// 	.mapTriplets(triplet => triplet.attr * triplet.dstAttr)

		// 	val betweennessGraph = Graph(graph.vertices, tempBetweennessGraph.edges)

		// betweennessGraphForRoot(1)
		// val betweennessGraph = betweennessGraphForRoot(1)

		// betweennessGraph.triplets.foreach(triplet => {
		// 	println( "(" + triplet.srcId + ", " + triplet.srcAttr + 
		// 		") -(" + triplet.attr + ")-> (" + triplet.dstId + 
		// 		", " + triplet.dstAttr + ")")
		// })

	}
}