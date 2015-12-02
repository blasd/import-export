# Introduction

The Import/Export tool was created as a collaborative effort between GigaSpaces Professional Services and GigaSpaces Support to facilitate reliable and efficient snapshots of data in a XAP datagrid.

Instructions on how to use the Import/Export tool and how it was implemented can be found on GigaSpaces [Best Practices](http://docs.gigaspaces.com/sbp/export-import-tool.html) wiki.

### Build and Running the Tool
#### PreRequisites
 * Java IDE
 * JDK 1.7
 * Apache Maven
 * [Install XAP Maven Plugin](http://docs.gigaspaces.com/xap102/maven-plugin.html)

##### Configuration
Check the pom.xml file to ensure `gsVersion` is configured to use the same version of XAP you installed with the Maven Plugin.

> To determine the exact version number you will need to pull it from within `<XAP_INSTALL_DIR>/tools/maven/maven-openspaces-plugin/pom.xml`
> Alternatively you can find the version as a directory name in your .m2 folder `.m2/repository/com/gigaspaces/gs-openspaces`

````xml
    <properties>
        <gsVersion>9.7.2-11000-RELEASE</gsVersion>
        ...
    </properties>
```


##### Building
```sh
mvn clean package
```

##### Running
 1. Copy the newly build export-import-1.0.SNAPSHOT.jar into the lib directory.
 2. Create a Shell, Batch, or Powershell script with the following

```sh
#! /bin/bash

. ./setAppEnv.sh

$JAVA_HOME\bin\java -cp $GS_HOME/lib/required/*:./lib/* com.gigaspaces.tools.importexport.Program -o export -l $LOOKUPLOCATORS -s mySpace -d /var/exporter/output
```