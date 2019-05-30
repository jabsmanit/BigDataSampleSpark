# SparkGeneralApplication
This is a sample Spark program in Java.
It demonstrate parallelizing of an existing collection to be used as RDD.

Steps for execution:
1) Build the SparkGeneralApplication maven project.
   You can either import the project into eclipse & build it or if maven is installed on your laptop, you can build it from command prompt.
   For building from command prompt, navigate to the root directory of the project and fire the command
    # mvn clean install
2) Navigate to the target directory on VM and run the spark application
    # spark-submit --class com.jabs.spark.ParallelizedCollectionDriver --master yarn SparkGeneralApplication-0.0.1-SNAPSHOT.jar
	
For WordCountDriver,
    # spark-submit --class com.jabs.spark.WordCountDriver --master yarn SparkGeneralApplication-0.0.1-SNAPSHOT.jar /HadoopWordCount/input/ /Spark/Output
	See the result
	# hdfs dfs -ls /Spark/Output
