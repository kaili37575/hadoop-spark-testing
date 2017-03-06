All programs have been tested successfully in Standalone Mode.
Question 1:
vc.jar and vm.jar have been tested successfully in AWS.
Question 2:
wc1.jar  WordCount1.java
wc2.jar  WordCount2.java
Question 3:
ut.jar   ustax.java

Running instruction(Standalone Mode):
step 1: get into hadoop-2.7.3 directory
step 2: 
type “bin/hadoop jar vc.jar inputPath outputPath”
type “bin/hadoop jar vm.jar inputPath outputPath”
type “bin/hadoop jar wc1.jar WordCount1 inputPath outputPath”
type “bin/hadoop jar wc2.jar WordCount2 inputPath outputPath”
type “bin/hadoop jar ut.jar UStax inputPath outputPath”


Note: You don’t need to specify the main class when running vc.jar and vm.jar
      OutputPath should not be existed
/////////////Compile java source code
step 1: get into hadoop-2.7.3 directory
step 2: type bin/hadoop com.sun.tools.javac.Main MyJavaFile
step 3: pack into a jar file
type “jar cf wc1.jar WordCount1*.class” to pack all WordCount1 class to wc1.jar file

////////////Running on AWS
step 1: upload files into s3 buckets
create folder input to store input files 
create folder log to store log info
create jar to store custom jar files 
step 2: create cluster
create a cluster in EMR
step 3: add steps
after cluster is successfully created, click add steps, then specify mown jar files.
type input and output directory path in Arguments part.
Note: output directory should not be existed 
then waiting for output