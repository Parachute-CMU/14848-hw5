# HW5 - Inverted Index on Apache Spark

## Scala Code for the inverted index algorithm
```scala
import org.apache.spark.{ SparkConf, SparkContext }


object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Inverted Index on Apache Spark").setMaster("local")
    val sc = new SparkContext(conf)
    val rootPath = "/Users/parachute/14848/hw5/"
    val inputPaths = rootPath + "Data/Hugo/*," + rootPath + "Data/shakespeare/*," + rootPath + "Data/Tolstoy/*"
    val stopPath = rootPath + "stop.txt"
    val outputPath = rootPath + "result"

    val stopWords = sc.textFile(stopPath).flatMap(x => x.split("\\r?\\n")).map(_.trim)
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)

    sc.wholeTextFiles(inputPaths).flatMap {
      case (path, content) =>
        content.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
          .split("""\W+""")
          .filter(!broadcastStopWords.value.contains(_)) map {
          // generate (word, filePath) tuple
          word => (word, path)
        }
    }.map {
      // count every tuple once
      case (word, path) => ((word, path), 1)
    }.reduceByKey {
      // count by word
      case (c1, c2) => c1 + c2
    }.map {
      // value is (path, count)
      case ((word, path), c) => (word, (path, c))
    }.groupBy {
      // group by word
      case (word, (path, count)) => word
    }.map {
      // format the output result
      case (word, res) =>
        val result = res map {
          case (_, (p, n)) => (p, n)
        }
        (word, result.mkString(", "))
    }.saveAsTextFile(outputPath)
  }
}
```
### Copy of the output
**result/part-00000**
