# Introduction

With this tool we will demonstrate how to export data from a space via serializing it to a file. We can then re-import the data back into the space. The tool executes distributed tasks in 'preprocess' mode, which reads the serialization files and returns a de-duplicated list of the classes only.

![xap-export-import.png](http://docs.gigaspaces.com/attachment_files/import-export-tool.jpg)

# Getting started

### Build and Running the Tool

##### Step 1: Setup XAP maven plugin
[How to setup XAP maven plugin](http://wiki.gigaspaces.com/wiki/display/XAP9/Maven+Plugin)

##### Step 2: Modify XAMP version in pom.xml file
<br>
Modify `<gsVersion>` within the `pom.xml` to include the right XAP release - below example having XAP 9.7 (9.7.0-10496-RELEASE) as the `<gsVersion>` value:

````xml
<properties>
        <gsVersion>9.7.0-10496-RELEASE</gsVersion>
        <springVersion>3.1.3.RELEASE</springVersion>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
</properties>
```


##### Step 3: Build the project
<br>
```sh
cd <project_root>
mvn clean install
```

##### Step 4: Deploy a space and write some data
<br>
If you do not have an already deployed space with data, you will need to deploy a new space and write some dummy data to it.

##### Step 5:	Run the tool to export the objects
<br>
```sh
cd target
java -classpath /home/adminuser/gigaspaces-xap-premium-10.1.1-ga/lib/required/*:/home/adminuser/gigaspaces-xap-premium-10.1.1-ga/lib/platform/benchmark/*:export-import-1.0-SNAPSHOT.jar:../lib/* com.gigaspaces.tools.importexport.SpaceDataImportExportMain -e -n 2 -s space -g pavlo -d /tmp/gs
```

In this case we are using a simple MessagePOJO from XAP benchmark.
Make sure that classes of the POJOs are set in the classpath before running.



```console
2015-07-14 17:47:08,432  INFO [com.gigaspaces.common] - (tid-821) : found 1 classes
2015-07-14 17:47:08,449  INFO [com.gigaspaces.common] - (tid-821) : starting export to file /tmp/gs/com.j_spaces.examples.benchmark.messages.MessagePOJO.1.1.ser.gz
2015-07-14 17:47:08,449  INFO [com.gigaspaces.common] - (tid-821) : starting export thread for com.j_spaces.examples.benchmark.messages.MessagePOJO
2015-07-14 17:47:08,449  INFO [com.gigaspaces.common] - (tid-821) : starting export to file /tmp/gs/com.j_spaces.examples.benchmark.messages.MessagePOJO.1.2.ser.gz
2015-07-14 17:47:08,450  INFO [com.gigaspaces.common] - (tid-821) : starting export thread for com.j_spaces.examples.benchmark.messages.MessagePOJO
2015-07-14 17:47:08,450  INFO [com.gigaspaces.common] - (tid-821) : waiting for 2 import operations to complete-complete
2015-07-14 17:47:08,450  INFO [com.gigaspaces.common] - (tid-938) : reading space class : com.j_spaces.examples.benchmark.messages.MessagePOJO
2015-07-14 17:47:08,451  INFO [com.gigaspaces.common] - (tid-938) : space partition contains 5000 objects
2015-07-14 17:47:08,451  INFO [com.gigaspaces.common] - (tid-938) : writing to file : /tmp/gs/com.j_spaces.examples.benchmark.messages.MessagePOJO.1.1.ser.gz
2015-07-14 17:47:08,461  INFO [com.gigaspaces.common] - (tid-938) : read 5000 objects from space partition
2015-07-14 17:47:08,461  INFO [com.gigaspaces.common] - (tid-938) : export operation took 24 millis
2015-07-14 17:47:08,462  INFO [com.gigaspaces.common] - (tid-938) : reading space class : com.j_spaces.examples.benchmark.messages.MessagePOJO
2015-07-14 17:47:08,462  INFO [com.gigaspaces.common] - (tid-938) : space partition contains 5000 objects
2015-07-14 17:47:08,462  INFO [com.gigaspaces.common] - (tid-938) : writing to file : /tmp/gs/com.j_spaces.examples.benchmark.messages.MessagePOJO.1.2.ser.gz
2015-07-14 17:47:08,463  INFO [com.gigaspaces.common] - (tid-938) : read 5000 objects from space partition
2015-07-14 17:47:08,463  INFO [com.gigaspaces.common] - (tid-938) : export operation took 22 millis
2015-07-14 17:47:08,465  INFO [com.gigaspaces.common] - (tid-821) : finished writing 1 classes
```

For each exported space class data `/tmp/gs`(or any other directory you've specified) will have a zip file with the class instances content.

##### Step 6:	Run the tool to import the objects back into a space<br/>
<br>
Once you restart the data grid you can reload your data back. This will reload the data from the zip files into the space:
```sh
cd target
java -classpath /home/adminuser/gigaspaces-xap-premium-10.1.1-ga/lib/required/*:/home/adminuser/gigaspaces-xap-premium-10.1.1-ga/lib/platform/benchmark/*:export-import-1.0-SNAPSHOT.jar:../lib/* com.gigaspaces.tools.importexport.SpaceDataImportExportMain -i -s space -g pavlo -d /tmp/gs
```

A space read call for each class is executed before trying to perform any import.

## Options
The tool supports the following arguments:


| Short Name               | Long Name          | Optional/required | Default value                        | Description|
|--------------------------|--------------------|-------------------|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| -e                       | --export        | optional          | NA                                   | Performs space class export                                                                                                            |
| -i                       | --import | optional          | NA                                   | Performs space class import                                                                                                                  |
| -l                       | ---locators  | optional          | NA                                   | Comma separated list of lookup locators (ex. 127.0.0.1:4174,192.168.1.100).                                                             |
| -g                       | --groups    | optional          | NA                                   | Comma separated list of lookup groups (ex. skyler,xap97).                                                                               |
| -s                       | --space    | required          | NA                                   | The name of the space                                                                                                                   | | -c                       | --classes     | optional          | NA                                | The classes whose objects to import/export - comma separated                                                                              |
| -b                       | --batch          | optional          | 1000                                   | The batch size                                                                                                                    |
| -p                       | --partitions          | optional          | NA                                   | The partition(s) to restore - comma separated                                                                               |
| -n                       | --number          | optional          | NA                                   | Number of partitions to export. Fro instance: now space has 4 partitions, but you wanto export all the data to space with 3 partitions, then "-n 3" has to be specified                                                                               |