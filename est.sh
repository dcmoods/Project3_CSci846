#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

hadoop com.sun.tools.javac.Main Project3_846/src/Estimator.java Project3_846/src/Pair.java
jar cf est.jar Project3_846/src/Estimator*.class Project3_846/src/Pair*.class

