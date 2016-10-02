# Step-by-Step guide to run Spark jobs on a Mesos cluster
In this guide we are run i.e. start, stop and verify the Mesos cluster setup. Additionally in the end we are actually submitting spark jobs to the cluster.
(Please note that we are using the directory structures and path setup from "Step-by-Step installation guide for a LAN Mesos cluster" guide.)

For our SparExperiments and SparkScalaExperiments we setup a Mesos cluster where we could submit jobs to. 
The cluster contained one master and three agents.
As hardware base we used three MacBook Pro (MBP) with 8 GB / 16 GB RAM and 8 CPU Cores each.

* One MBP ran the Mesos master and one agent
* One MBP ran one Mesos agent and was used for submitting the spark jobs and provided the spark binaries via HTTP
* One MBP ran just the agent

The release version of Mesos was: 1.0.1

As an entry point we used the following guides: 
* Example section at the end from http://mesos.apache.org/gettingstarted/
* Overview and some instructions from here: http://spark.apache.org/docs/latest/running-on-mesos.html
* For the actual submit of spark jobs to the cluster: http://spark.apache.org/docs/latest/submitting-applications.html

## Prerequisites
### An comment on hostname vs. ip-address based setup
The communication between Mesos master and agents is a standard client-server setup via port 5050 on the master and dynamic ports on agents. For this communication (and the provisioning of the Spark binary package - see below) you have to adress the machines. E.g. to submit a Spark job to the Mesos master you have to specify the master URL via ```--master``` parameter in the form ```mesos://<ip-address or hostname>:5050```. 

The setup via hostnames or via ip-adress both worked out well.

In case of ip-address based setup you have to ensure that each machine gets the same ip-address assigned every time. 
In case of hostnames you have to ensure that the hostnames are resolvable within your LAN. Either you have some service within your LAN to do that for your (we had that) or you have to assign the ip-addresses to hostnames in /etc/hosts on every machine as described in the original Mesos and Spark guides.

### Providing the Spark binary package
To run a Spark job on an Mesos agent the agent needs the Spark executor binaries available on local storage. To provide the binaries on run time to the distributed agents we made them available via HTTP. As a web server we used MAMP and put the spark binaries from https://spark.apache.org/downloads.html (Spark 2.0.0, Pre-build for Hadoop 2.7 and later) in the htdocs root. So the other agents could reach to package via http://ip-adress-or-hostname:webserver-port/spark-2.0.0-bin-hadoop2.7.tgz

### Install Spark 
For the installation of Spark just extract the binary package you have just downloaded to a additional location of your choice. We copied the extracted files to ```/Applications/spark-2.0.0-bin-hadoop2.7```. 

#### Provide the Spark binary package and libmesos to shell executables
If you want to use the Spark shell programs and don't want to set the path to the Spark binaries and libmesos in each and every command you can export the corresponding path parameters ```MESOS_NATIVE_JAVA_LIBRARY``` and ```SPARK_EXECUTOR_URI``` to your shell environment.
```
# Export the path to libmesos (*.dylib for Mac OSX; *.so else)
$ export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/libmesos.dylib

# Export the path to Spark binaries URL
$ export SPARK_EXECUTOR_URI=http://10.89.0.56:8888/spark-2.0.0-bin-hadoop2.7.tgz
```

### Storage of input / output files
Within our initial setup we didn't try to setup HDFS. Therefore we needed a common share for the input and output files which is reachable from all Mesos agents (and Spark executors). We decided to store the files on one MBP (the one which submitted the Spark jobs) and share them via afp:// (smb://, ntfs://) to the other agents.
 To set this up:
 ```
 # Create a directoriy on each agent machine with exactly the same path (may need 'sudo')
 $ mkdir /Applications/24h_Stunden_Sprint/share
 
 # Make sure the directory is owned by the mesos user (may need 'sudo')
 $ chown mesos:admin /Applications/24h_Stunden_Sprint/share
 
 # On the submitting machine: 
 # Store all input files within that folder
 # Share the folder to the other agents: 
 # We used the OSX sharing settings panel (Freigaben) and file sharing (Dateifreigabe)
 # Share the folder /Applications/24h_Stunden_Sprint/share to the user mesos for read and write
 
 # On the other agent machines mount the provided share 
 $ mount -t afp afp://mesos:<pwd>@<submitting_agent_machine>/share /Applications/24h_Stunden_Sprint/share
 # e.g.
 # mount -t afp afp://mesos:mesos@10.89.0.56/share /Applications/24h_Stunden_Sprint/share
 ```
 
 ### Changes to the Java / Scala code (optional)
 See the Java code in the main class kreyling.importer.counterparty.CounterPartyImporter as an example.
 
 In general all of the configuration done in ```SparkConf``` can be externalized as [command line parameters on the spark-submit job](http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit). Then there is no code change needed. We use a mixed here to show you both options.
 
 ```
 SparkConf conf = new SparkConf()
                 .setAppName(CounterPartyImporter.class.getSimpleName())
                 // Overwrite output files
                 .set("spark.hadoop.validateOutputSpecs", "false")
                 //Download spark binaries from here
                 .set("spark.executor.uri", "http://10.89.0.56:8888/spark-2.0.0-bin-hadoop2.7.tgz"); 
```

As you can see there is no configuration of e.g. the Spark master URL here because we externalized that as command line arguments (see below). But if you want to you can also do an ```.setMaster("mesos://10.89.0.56:5050")```here.

### Package the Java (/Scala) code to executable jars
To get the jars of the code which we will submit to Spark on the Mesos cluster later on we used plain old ```maven install``` (nothing special here ;-)

## Starting the Mesos cluster
After all that configuration starting the mesos master and agents is straight forward.

### Starting the Mesos master
```
# Go to your install (build) directory
$ cd /Applications/mesos-1.0.1/build

# Starting the Mesos master on standard port 5050
$ ./bin/mesos-master.sh --work_dir=/var/lib/mesos  
```

### Starting the Mesos agent
Do that on every agent machine.

The switch_user param is used to prevent mesos to change to the user for executing the job which has submitted the job. So it sticks to the user 'mesos' for execution. 
```
# Go to your install (build) directory
$ cd /Applications/mesos-1.0.1/build

# Starting the Mesos agent
$ ./bin/mesos-agent.sh --master=10.89.0.96:5050 --work_dir=/var/lib/mesos-agent --switch_user=false  
```

### Verfiy that agents are connected
On the Mesos master machine we can switch to the web UI: http://localhost:5050.
Now we should see the agents connected to the master on agent's tab.

## Shutting down Mesos master and agents
If you have to shutdown the Mesos master or agent just send CTRL-C in the shell.

## Run Spark jobs on the Mesos cluster
As a first test we are running the spark REPL on the mesos cluster

### Run the Spark Shell
``` 
# Change to Spark install location 
$ cd /Applications/spark-2.0.0-bin-hadoop2.7

# Run the Spark shell
./bin/spark-shell --master mesos://host:5050
```
After startup we should see \<number of agents\>  times task running in the Mesos web UI. 
For some initial tests you cann do in the shell see: http://spark.apache.org/docs/latest/quick-start.html

### Submitting own programs as Spark jobs to the cluster
Finally we want to submit our own programs as Spark jobs to the cluster. To do that we are using the [spark-submit command with the following parameters](http://spark.apache.org/docs/latest/submitting-applications.html#submitting-applications):
```
# Change to Spark install location 
$ cd /Applications/spark-2.0.0-bin-hadoop2.7

# Submit the job
$ ./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  <application-jar> \
  [application-arguments]
```
In our setup the concrete parameters are
* main-class: kreyling.importer.counterparty.CounterPartyImporter
* master-url: mesos://10.89.0.56:5050
* application-jar: /Applications/24h_Stunden_Sprint_2016/SparkExperiments/target/SparkExperiments-1.0-SNAPSHOT.jar

Additionally - as stated above - we can also directly set some configuration parameters instead of specifying them in the code. In our case this could be: 
```
./bin/spark-submit \
  --class kreyling.importer.counterparty.CounterPartyImporter \
  --master mesos://10.89.0.56:5050 \
  --conf spark.hadoop.validateOutputSpecs=false \
  --conf spark.executor.uri="http://10.89.0.56:8888/spark-2.0.0-bin-hadoop2.7.tgz" \
  /Applications/24h_Stunden_Sprint_2016/SparkExperiments/target/SparkExperiments-1.0-SNAPSHOT.jar
```

In the Mesos web UI the submitted Spark job gets visible as Mesos 'framework' which is controlled by the Spark driver. As a result we should see the running tasks in the Mesos web UI distributed on the agents.

After finishing the distributed jobs the results get written back to the Spark driver, which tries to shut down the Mesos framework and Spark executors afterwards. In our case there were some exceptions in the drivers output, but they were only stating that the executors could not be shut down in the first try.
```
16/09/30 13:52:27 WARN NettyRpcEndpointRef: Error sending message [message = RemoveExecutor(1,Executor finished with state FINISHED)] in 3 attempts
org.apache.spark.SparkException: Exception thrown in awaitResult
```

At the end the Spark driver exists and we can see the results in the input / output folder: ```/Applications/24h_Stunden_Sprint/share```

In our example case (CounterPartyImporter) the result gets stored in the directory 'CIS.txt' as partial results: 
```
Philipps-MBP:share mesos$ ls -al CIS.txt/
total 328
drwxr-xr-x   8 mesos  admin    272 Sep 30 13:22 .
drwxr-xr-x  13 mesos  admin    442 Sep 30 13:25 ..
-rw-r--r--   1 mesos  admin      8 Sep 30 13:22 ._SUCCESS.crc
-rw-r--r--   1 mesos  admin    608 Sep 30 13:22 .part-00000.crc
-rw-r--r--   1 mesos  admin    608 Sep 30 13:22 .part-00001.crc
-rw-r--r--   1 mesos  admin      0 Sep 30 13:22 _SUCCESS
-rw-r--r--   1 mesos  admin  76580 Sep 30 13:22 part-00000
-rw-r--r--   1 mesos  admin  76489 Sep 30 13:22 part-00001
```
The results are stored in the part-0000\* files. 
!YEAAHH!
