package edu.cornell.tech.cs5304.lab2


import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths._
import scala.reflect.ClassTag

object GirvanNewman {

	def run[VD, ED: ClassTag](graph: Graph[VD, ED]): Unit = {

		graph.cache()
		val landmarks: Seq[VertexId] = graph.vertices.collect.map(pair => pair._1).toSeq
		// val landmarks: Seq[VertexId] = Seq(0)

		val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, landmarks)

		// Array[(VertexId, Map[VertexId, Int])]
		val source_id = 0

		// println("Shortest Path from " + source_id)
		// shortestPaths.vertices.collect
		// .foreach{ case (vid:VertexId, pathMap:Map[VertexId, Int]) => {
		// 		println(source_id + " -> " + vid + ": " + pathMap(source_id))
		// 	}
		// }

		val shortestPathsMap: Map[VertexId, Map[VertexId, Int]] = shortestPaths.vertices.collect.toMap
		shortestPathsMap(source_id).foreach { case (vid:VertexId, length:Int) => {
				println(source_id + " -> " + vid + ": " + length)
			}
		}

		//for each vertex

		


		//Init all vertices in group 1 = 1

		//for each group, where g begins at g=2
		//send messages from nodes in g-1 to g (connected), total

		//to make this easier, we can construct a new graph of only edges where g-1 -> g, i.e., path length is increasing by 1

		val sourceShortestPathMap = shortestPathsMap(source_id)
		val shortestPathGraph = graph.subgraph(epred = (triplet) => (sourceShortestPathMap(triplet.srcId)+1 == sourceShortestPathMap(triplet.dstId)))

		//then we can use the preggle api to do the rest


	}
}