import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConversions._

val TASK_MEMORY_LIMIT = 100000000L
val HDFS_HOST = "nodeXYZ.ib.cluster"
val OUTPUT_PATH = "/var/scratch/USER/SC19-analysis/"

case class TaskProperty(val taskType: String, val numChildren: Long, val numParents: Long)

def analyzeTrace(inputPath: String, localOutputPath: Path): Unit = {
  val traceDF = spark.read.parquet(inputPath)
  
  val traceVertices = if (traceDF.columns.contains("type")) {
    traceDF.select("id", "type").rdd.map(row => {
      val id = row(0).asInstanceOf[Long]
      val taskType = row(1).asInstanceOf[String]
      (id, TaskProperty(taskType, 0L, 0L))
    })
  } else {
    traceDF.select("id").rdd.map(row => {
      val id = row(0).asInstanceOf[Long]
      val taskType = ""
      (id, TaskProperty(taskType, 0L, 0L))
    })
  }
  val traceEdges = traceDF.select("id", "children").rdd.flatMap(row => {
    val id = row(0).asInstanceOf[Long]
    val children = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[Long]]
    children.map(child => Edge(id, child, true))
  })
  
  val taskCount = traceVertices.count()
  println(s"Processing $inputPath with $taskCount tasks...")
  
  var traceGraph = if (taskCount >= TASK_MEMORY_LIMIT) {
    Graph(traceVertices, traceEdges, null.asInstanceOf[TaskProperty], StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK).cache()
  } else {
    Graph(traceVertices, traceEdges).cache()
  }
  
  // EXAMPLE GRAPH TO TEST IMPLEMENTATION
  //val traceVertices = sc.parallelize(Array(
  //  (1L, TaskProperty("task", 0L, 0L)), (2L, TaskProperty("task", 0L, 0L)),
  //  (3L, TaskProperty("dummy", 0L, 0L)),
  //  (4L, TaskProperty("dummy", 0L, 0L)), (5L, TaskProperty("dummy", 0L, 0L)),
  //  (6L, TaskProperty("task", 0L, 0L)), (7L, TaskProperty("task", 0L, 0L)), (8L, TaskProperty("task", 0L, 0L)), (9L, TaskProperty("task", 0L, 0L)), (10L, TaskProperty("task", 0L, 0L)),
  //  (11L, TaskProperty("task", 0L, 0L))
  //))
  //val traceEdges = sc.parallelize(Array(
  //  Edge(1L, 3L, true), Edge(2L, 3L, true),
  //  Edge(3L, 4L, true), Edge(3L, 5L, true),
  //  Edge(4L, 6L, true), Edge(4L, 7L, true), Edge(4L, 8L, true), Edge(5L, 9L, true), Edge(5L, 10L, true),
  //  Edge(6L, 11L, true), Edge(7L, 11L, true), Edge(8L, 11L, true), Edge(9L, 11L, true), Edge(10L, 11L, true)
  //))
  //var traceGraph = Graph(traceVertices, traceEdges).cache()
  
  // Send initial messages 
  var messages = traceGraph.aggregateMessages[(Long, Long)](
    // Message format: (num children, num parents)
    triplet => {
      triplet.srcAttr match {
  	  case TaskProperty("dummy", _, _) => triplet.sendToDst((0L, 0L))
      case TaskProperty(_, _, _) => triplet.sendToDst((0L, 1L))
  	}
  	triplet.dstAttr match {
  	  case TaskProperty("dummy", _, _) => triplet.sendToSrc((0L, 0L))
      case TaskProperty(_, _, _) => triplet.sendToSrc((1L, 0L))
  	}
    },
    (a, b) => (a._1 + b._1, a._2 + b._2)
  ).cache()
  
  while (!messages.filter { case (vid, msg) => msg._1 > 0 || msg._2 > 0 }.isEmpty()) {
    // Merge messages with graph
    var oldG = traceGraph
    traceGraph = traceGraph.joinVertices(messages)(
      (vid, vprop, msg) => vprop match {
        case TaskProperty("dummy", _, _) => TaskProperty("dummy", msg._1, msg._2)
        case TaskProperty(taskType, nc, np) => TaskProperty(taskType, nc + msg._1, np + msg._2)
      }
    ).cache()
    
    // Handle RDD caching
    traceGraph.vertices.count()
    traceGraph.edges.count()
    oldG.unpersist()
    messages.unpersist()
    
    // Compute new messages
    messages = traceGraph.aggregateMessages[(Long, Long)](
      // Message format: (num children, num parents)
      triplet => {
        triplet.srcAttr match {
          case TaskProperty("dummy", _, np) => triplet.sendToDst((0L, np))
          case TaskProperty(_, _, _) => triplet.sendToDst((0L, 0L))
        }
        triplet.dstAttr match {
          case TaskProperty("dummy", nc, _) => triplet.sendToSrc((nc, 0L))
          case TaskProperty(_, _, _) => triplet.sendToSrc((0L, 0L))
        }
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    ).cache()
  }
  
  // Translate child and parent counts to human-readable relationship expressions
  def zeroOneMany(value: Long): String = {
    if (value == 0) "0"
    else if (value == 1) "1"
    else if (value > 1) "N"
    else throw new IllegalArgumentException("Value was negative")
  }
  val taskRelationships = traceGraph.vertices.flatMap { case (vid, vprop) => vprop match {
    case TaskProperty("dummy", _, _) => Seq()
    case TaskProperty(_, nc, np) => {
      Seq((vid, s"${zeroOneMany(np)}:${zeroOneMany(nc)}"))
    }
  }}.cache()
  
  // Store final result to a file
  val finalResult = taskRelationships.map { case (vid, relationship) => (relationship, 1) }.reduceByKey(_ + _).collect()
  Files.write(localOutputPath, asJavaIterable(finalResult.toIterable), StandardCharsets.UTF_8)
  
  // Handle RDD caching
  taskRelationships.unpersist()
  traceGraph.unpersist()
  messages.unpersist()
}

val TRACES = Array(
  "askalon_ee2_parquet",
  "askalon_ee_parquet",
  "askalon-new_ee10_parquet",
  "askalon-new_ee11_parquet",
  "askalon-new_ee12_parquet",
  "askalon-new_ee13_parquet",
  "askalon-new_ee14_parquet",
  "askalon-new_ee15_parquet",
  "askalon-new_ee16_parquet",
  "askalon-new_ee17_parquet",
  "askalon-new_ee18_parquet",
  "askalon-new_ee19_parquet",
  "askalon-new_ee20_parquet",
  "askalon-new_ee21_parquet",
  "askalon-new_ee22_parquet",
  "askalon-new_ee23_parquet",
  "askalon-new_ee24_parquet",
  "askalon-new_ee25_parquet",
  "askalon-new_ee26_parquet",
  "askalon-new_ee27_parquet",
  "askalon-new_ee28_parquet",
  "askalon-new_ee29_parquet",
  "askalon-new_ee30_parquet",
  "askalon-new_ee31_parquet",
  "askalon-new_ee32_parquet",
  "askalon-new_ee33_parquet",
  "askalon-new_ee34_parquet",
  "askalon-new_ee35_parquet",
  "askalon-new_ee36_parquet",
  "askalon-new_ee37_parquet",
  "askalon-new_ee38_parquet",
  "askalon-new_ee39_parquet",
  "askalon-new_ee3_parquet",
  "askalon-new_ee40_parquet",
  "askalon-new_ee41_parquet",
  "askalon-new_ee42_parquet",
  "askalon-new_ee43_parquet",
  "askalon-new_ee44_parquet",
  "askalon-new_ee45_parquet",
  "askalon-new_ee46_parquet",
  "askalon-new_ee47_parquet",
  "askalon-new_ee48_parquet",
  "askalon-new_ee49_parquet",
  "askalon-new_ee4_parquet",
  "askalon-new_ee50_parquet",
  "askalon-new_ee51_parquet",
  "askalon-new_ee52_parquet",
  "askalon-new_ee53_parquet",
  "askalon-new_ee54_parquet",
  "askalon-new_ee55_parquet",
  "askalon-new_ee56_parquet",
  "askalon-new_ee57_parquet",
  "askalon-new_ee58_parquet",
  "askalon-new_ee59_parquet",
  "askalon-new_ee5_parquet",
  "askalon-new_ee60_parquet",
  "askalon-new_ee61_parquet",
  "askalon-new_ee62_parquet",
  "askalon-new_ee63_parquet",
  "askalon-new_ee64_parquet",
  "askalon-new_ee65_parquet",
  "askalon-new_ee66_parquet",
  "askalon-new_ee67_parquet",
  "askalon-new_ee68_parquet",
  "askalon-new_ee69_parquet",
  "askalon-new_ee6_parquet",
  "askalon-new_ee7_parquet",
  "askalon-new_ee8_parquet",
  "askalon-new_ee9_parquet",
  "chronos_parquet",
  "Google_parquet",
  "icpe_trace-1_parquet",
  "icpe_trace-2_parquet",
  "LANL_Trinity_parquet",
  "pegasus_P1_parquet",
  "pegasus_P2_parquet",
  "pegasus_P3_parquet",
  "pegasus_P4_parquet",
  "pegasus_P5_parquet",
  "pegasus_P6a_parquet",
  "pegasus_P6b_parquet",
  "pegasus_P7_parquet",
  "pegasus_P8_parquet",
  "Two_Sigma_dft",
  "Two_Sigma_pit",
  "workflowhub_epigenomics_dataset-hep_chameleon-cloud_schema-0-2_epigenomics-hep-100000-cc-run005_parquet",
  "workflowhub_epigenomics_dataset-hep_futuregrid_schema-0-2_epigenomics-hep-fg-run001_parquet",
  "workflowhub_epigenomics_dataset-hep_grid5000_schema-0-2_epigenomics-hep-g5k-run001_parquet",
  "workflowhub_epigenomics_dataset-ilmn_chameleon-cloud_schema-0-2_epigenomics-ilmn-100000-cc-run004_parquet",
  "workflowhub_epigenomics_dataset-taq_chameleon-cloud_schema-0-2_epigenomics-taq-100000-cc-run002_parquet",
  "workflowhub_montage_dataset-02_degree-2.0_osg_schema-0-2_montage-2.0-osg-run007_parquet",
  "workflowhub_montage_dataset-02_degree-4.0_osg_schema-0-2_montage-4.0-osg-run009_parquet",
  "workflowhub_montage_ti01-971107n_degree-2.0_osg_schema-0-2_montage-2.0-osg-run007_parquet",
  "workflowhub_montage_ti01-971107n_degree-4.0_osg_schema-0-2_montage-4.0-osg-run009_parquet",
  "workflowhub_soykb_grid5000_schema-0-2_soykb-g5k-run002_parquet"
)

TRACES.foreach(trace => {
  val inputPath = s"hdfs://${HDFS_HOST}:9000/${trace}/tasks/schema-1.0"
  val outputPath = Paths.get(OUTPUT_PATH).resolve(trace)
  analyzeTrace(inputPath, outputPath)
})















// WORKFLOWHUB:
//import java.nio.file.{Files, Path, Paths}
//import java.nio.charset.StandardCharsets
//
//import scala.collection.JavaConversions._
//
//val TRACES = Array(
//  "workflowhub_epigenomics_dataset-hep_chameleon-cloud_schema-0-2_epigenomics-hep-100000-cc-run005_parquet",
//  "workflowhub_epigenomics_dataset-hep_futuregrid_schema-0-2_epigenomics-hep-fg-run001_parquet",
//  "workflowhub_epigenomics_dataset-hep_grid5000_schema-0-2_epigenomics-hep-g5k-run001_parquet",
//  "workflowhub_epigenomics_dataset-ilmn_chameleon-cloud_schema-0-2_epigenomics-ilmn-100000-cc-run004_parquet",
//  "workflowhub_epigenomics_dataset-taq_chameleon-cloud_schema-0-2_epigenomics-taq-100000-cc-run002_parquet",
//  "workflowhub_montage_dataset-02_degree-2.0_osg_schema-0-2_montage-2.0-osg-run007_parquet",
//  "workflowhub_montage_dataset-02_degree-4.0_osg_schema-0-2_montage-4.0-osg-run009_parquet",
//  "workflowhub_montage_ti01-971107n_degree-2.0_osg_schema-0-2_montage-2.0-osg-run007_parquet",
//  "workflowhub_montage_ti01-971107n_degree-4.0_osg_schema-0-2_montage-4.0-osg-run009_parquet",
//  "workflowhub_soykb_grid5000_schema-0-2_soykb-g5k-run002_parquet"
//)
//val INPUT_PATH = "/local/USER/"
//
//def zeroOneMany(value: Int): String = {
//  if (value == 0) "0"
//  else if (value == 1) "1"
//  else if (value > 1) "N"
//  else throw new IllegalArgumentException("Value was negative")
//}
//
//TRACES.foreach(trace => {
//  val tracePath = INPUT_PATH + trace + "/tasks/schema-1.0"
//  val DF = spark.read.parquet(tracePath)
//  val relationships = DF.select(size($"children").as("num_children"), size($"parents").as("num_parents")).rdd.map(row => {
//    val nc = row(0).asInstanceOf[Int]
//    val np = row(1).asInstanceOf[Int]
//    s"${zeroOneMany(np)}:${zeroOneMany(nc)}"
//  })
//  val finalResult = relationships.map { relationship => (relationship, 1) }.reduceByKey(_ + _).map { case (relationship, count) => s"$relationship $count" }.collect()
//  Files.write(Paths.get(OUTPUT_PATH).resolve(trace), asJavaIterable(finalResult.toIterable), StandardCharsets.UTF_8)
//})

