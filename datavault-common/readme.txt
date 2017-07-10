Instructions for manually installing missing jars from maven repository

So far the missing jars are round the Oracle Cloud File Store


1) Maven POM
need to run the validate phase specifically

mvn validate 

then run 

mvn clean install

The build phase info is in the common pom.xml

2) To install manually rather than via maven pom

FTM

mvn install:install-file -Dfile=./ftm-sdk-2.2.5/ftm-api-2.2.5.jar -DgroupId=ftm-api -DartifactId=ftm-api -Dversion=2.2.5 -Dpackaging=jar
mvn install:install-file -Dfile=./ftm-sdk-2.2.5/javax.json-1.0.4.jar -DgroupId=javax.json -DartifactId=javax.json -Dversion=1.0.4 -Dpackaging=jar
mvn install:install-file -Dfile=./ftm-sdk-2.2.5/low-level-api-core-1.14.9.jar -DgroupId=low-level-api-core -DartifactId=low-level-api-core -Dversion=1.14.9 -Dpackaging=jar

SDK

mvn install:install-file -Dfile=./oracle-cloud-storage-sdk-13.1.3/oracle.cloud.storage.api-13.1.3.jar -DgroupId=oracle.cloud.storage.api -DartifactId=oracle.cloud.storage.api -Dversion=13.1.3 -Dpackaging=jar
