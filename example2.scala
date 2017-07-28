import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object thisthang {
  def main(args: Array[String]){
var con=new SparkConf()
var sc=new SparkContext(conf=con)

var rdd=sc.parallelize(array(1,2,3,4,5))
var theList=rdd.collect()

for(a <- theList){
println(a)
}
}}
