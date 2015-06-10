#Intro to Spark and Cassandra

## Running a Spark Project against the Spark Master included in DataStax Enterprise

In the next series of exercises we are going to be working the Spark Master that is started with DataStax Enterprise (DSE).  Be sure you have started DSE with Spark Enabled as specified in the main README in the root of the project directory.  That is you have at least one instance of DSE is running as an analytics node.

* Throughout all the exercises you can view information on the Spark Master and currently running jobs at [the Spark Master URL](http://localhost:7080/)

## Exercise 1 - Run a basic Spark Job against the DSE Spark Master

* Copy BasicSparkDemo.java from the previous exercises (the file in the sparkStarter project) into this projects src/main/java/simpleSpark directory.
  * The previous projects build was greatly simplified to make it easy to run standalone Spark Projects.
  * This project has a more complex build that assembles a uber jar file and prepares it for submitting to the DSE Spark Master.  This uses the maven-assembly-plugin to build the jar file.
  * Another critical difference is the scope of most of the dependencies are marked as provided because they are provided by the DSE Spark Master when the project is run.  By marking them as provided they are not included in the uber jar built in the assembly task.

* Modify the BasicSparkDemo so that the master is set to the URL of the DSE Spark Master.
  * Modify SparkConfSetup to point to the correct DSE_HOST - this is the IP of where DSE is running
  * Also in SparkConfSetup modify the DRIVER_HOST to point to the IP address where the program will be run.  This is most likely the computer where you are running the project.
  * Notice that the SparkConf sets the master to be "spark://DSE_HOST:7077" - That is when DSE is run in analytics modes the Spark Master is automatically started on that same server on the port 7077.  
  * Delete the getJavaSparkContext method and change the main program to use SparkConfSetup.getJavaSparkContext() instead.

* Build the Simple Spark Project in maven using:
  `mvn install`
* Run the Spark Project by submitting the job to the DSE Spark Master (modify the command to point to where you installed DSE):

  `~/dse-4.6.6/bin/dse spark-submit --class simpleSpark.BasicSparkDemo ./target/IntroSparkCassandra-0.0.1-SNAPSHOT.jar`

* [See these docs for details about dse spark-submit](http://docs.datastax.com/en/datastax_enterprise/4.6/datastax_enterprise/spark/sparkStart.html)

* Verify the program ran by going to the Spark Master Web Page and finding the stdout of the Spark Job.
  * Standard out will actually be in the log file of the Spark Worker and not in the terminal window where the job ran.
  * The same log file can be found on the server under the /var/lib/spark/work/app ...
  * The program might terminate with an ERROR - that doesn't mean the program didn't run.

## Exercise 2 - Connecting to Cassandra from Spark

* The BasicReadWriteDemo can be run using `compileAndRunBasicReadWriteDemo.sh`

* Notice in the SparkConf setup the host of cassandra is specified in spark.cassandra.connection.host.  This will establish a connection to that cassandra server.
* The most basic way to connect to Cassandra from Spark is to create a session and execute CQL on that session.
* A session can be retrieved from the CassandraConnector by calling openSession.
* When finished with the session be sure to close it using:

```
finally {
  if (session != null)
    session.close();
}
```

* In the basicCassandraSession method create a session and execute the following CQL.  

```
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }

CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)

TRUNCATE test.key_value
```

* Insert some test values into the key_value table.
* Also create the tables for the following exercises.  (This creates a secondary index which is not recommended.  It is only used for demonstration purposes.  Proper data model would be recommended instead.)

```
CREATE TABLE IF NOT EXISTS test.people (id INT, name TEXT, birth_date TIMESTAMP, PRIMARY KEY (id))

CREATE INDEX IF NOT EXISTS people_name_idx ON test.people(name)
```

* Run BasicReadWriteDemo using:

  `compileAndRunBasicReadWriteDemo.sh`

* This script uses dse spark-submit

* Verify the tables are created and the data inserted by going to cqlsh and verifying the data.

## Exercise 3 - Writing tables from a Spark JavaRDD into Cassandra

The goal of this exercise is to write the list of People in the method basicSparkAndCassandra into Cassandra.

* The static import `import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;` brings into scope most of the methods that are needed for working with the Spark Cassandra Connector in Java
* The most import method is `javaFunctions` which converts a JavaRDD to a RDDJavaFunctions.  That class is a Java API Wrapper around the standard RDD functions and adds functionality to interact with Cassandra.
* From the `RDDJavaFunctions` instance that is returned call the `writeBuilder` that defines the mapping of the RDD into Cassandra.  For example the following call:
  * `writerBuilder("test", "people", mapToRow(Person.class))`
  * Maps the write into the keyspace test, table people.  It then uses a Java Bean Mapper to map the Person Class to the proper columns in the people table.
* Finally call saveToCassandra to persist all of the entries in the RDD to Cassandra.

The full code that is need then is:

`javaFunctions(rdd).writerBuilder("test", "people", mapToRow(Person.class)).saveToCassandra();`


## Exercise 4 - Reading tables from Cassandra into a Spark JavaRDD

In the next exercise we are going to read data out of Cassandra into a Spark RDD.  

* Again using `javaFunctions` create a `SparkContextJavaFunctions`

  `javaFunctions(javaSparkContext)`

*  Using the `sparkContextJavaFunctions` load a Cassandra table into an Java RDD

  `cassandraTable("test", "people")`

*  Map each Cassandra row that is returned as a string and print out the rows.

* Use the `mapRowTo` parameter to map each row to a Person automatically:

  `cassandraTable("test", "people", mapRowTo(Person.class))`

* Add a filter to the rows.  This is what is called a "push down predicate" because the filter is executed on the server side before it is returned to Cassandra.  This is much more efficent then loaded all of the data into Spark and then filtering it.

* As an example add a call to the method  `.where("name=?", "Anna")` after the cassandraTable method.  Print out each person returned.

* Use a projection to select only certain columns from the table `select("id")`


## Exercise 5 - Word Count Demo - Counting the words in a novel

The goal of this exercise is to get familiar with the more advanced functionality of Spark by reading a text file and storing a count of the words in Cassandra.  An important part of this is learning to use Pair RDD's.

Additional Functionality is provided on RDD's that are key / value pairs.  These RDDs are called pair RDDs.  They provide additional APIs that are performed on each key in the RDD.  Some common examples are reduceByKey which performs a reduce function that operates on each key.  Or there is a join operation which joins two RDD's by key.  

Pair RDD's are defined using a Tuple of 2.  Tuples are defined as "a finite ordered list of elements".  They can be thought of as a generic container of data. In the case of Pair RDD's the first element is the key and the second element is the value.  

In Java a Pair RDD can be created using the method `mapToPair`.  It operates on each element in an RDD and returns a new element that is a Tuple2.   The method 'mapToPair' then expects a function that takes a single element and returns a pair of key and value.

* Copy the data directory into the /tmp directory on BOTH the Spark Driver and the Spark Master where DSE is running.  The program will try and read the text files from the /tmp/data directory.

* The SparkWordCount can be run using `compileAndRunWordCount.sh.sh`

* In the method sparkWordCount for each of the files in the constant  `DATA_FILES` read the file into a Spark RDD
  * `javaSparkContext.textFile(DATA_FILE_DIR + fileName);`


* Split each string into a list of words.  Hint - use flatMap and then split each line with `nextLine.split("\\s+")`

* Convert each word in the RDD to a Pair RDD of (Word, 1) using `mapToPair`.  The second value is what will be used to count each word in the list.

* Use `reduceByKey` to count all words.

* Map the results of the list to the `WordCountFileName` to make it easy to save out to Cassandra.

* Save the count of words to Cassandra

* Bonus filter out stop words to improve the accuracy of the count.

## Exercise 6 - Word Count Demo - Analyzing the results

* Read the results back out of the database for all datasets into WordCountFileName.

* Map the results to a Pair RDD with the count as the key

* Find the top word counts out of the RDD.


## Further Documentation

[Spark Cassandra Connector Java API Documentation](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/7_java_api.md)

[Accessing cassandra from spark in java](http://www.datastax.com/dev/blog/accessing-cassandra-from-spark-in-java)
