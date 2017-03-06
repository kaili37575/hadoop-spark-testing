All programs have been tested successfully in Local machine.
Question 1:
vc.jar and vm.jar have been tested successfully in AWS.
In hadoop assignment, I compute the mean voter by total_voters/num_records, but in Spark I compute the mean voter by total_voters/num_district.

Question 2:
wc1.jar  wordcount1.java
wc2.jar  wordcount2.java

In Question 2b, I set partition to be 1 to avoid shuffle when reading the TXT files. Otherwise, the lines read by JavaSparkContext().textFile() will be unordered. Since the files read by testFile() in different partition and then collect them together to store in JavaRDD. 

Question 3:
ustax.jar   ustax.java

I treat “AL 1” as one string to be to key, so the question is as easy as Question 2a.

Running instruction(Local machine):
add spark/bin and spark/sbin to $PATH, so we can use spark command directly.
————————————————————————————
e.g. Question 1a 
//Compile java file
javac -cp  /Users/likaiapply/spark-2.0.2-bin-hadoop2.7/jars/scala-library-2.11.8.jar:/Users/likaiapply/spark-2.0.2-bin-hadoop2.7/jars/spark-core_2.11-2.0.2.jar:/Users/likaiapply/spark-2.0.2-bin-hadoop2.7/jars/spark-sql_2.11-2.0.2.jar votercount.java 

//pack into a jar file
jar cf vc.jar votercount.class
jar cfm vc.jar manifest.txt voter count.class

//running program
spark-submit --class  "votercount" vc.jar input/a/ o7   //for which doesn’t include a manifest file;
spark-submit vc.jar input/a/ o7  //for which include a manifest file;

———————————————————————————

type “spark-submit vc.jar inputPath outputPath”
type “spark-submit vm.jar inputPath outputPath”
type “spark-submit --class  “wordcount1” wc1.jar inputPath outputPath”
type “spark-submit --class  “wordcount2” wc2.jar inputPath outputPath”
type “spark-submit --class  “ustax” ustax.jar inputPath outputPath”

————————————————————————————

Note: 1.You don’t need to specify the main class when running vc.jar and vm.jar
      2.OutputPath should not be existed


////////////Running on AWS
step 1: upload files into s3 buckets
create folder input to store input files 
create folder log to store log info
create jar to store custom jar files 
step 2: create cluster
create a cluster in EMR
step 3: add steps
after cluster is successfully created, click add steps, then specify myown jar files.
type input and output directory path in Arguments part.
Note: output directory should not be existed 
then waiting for output