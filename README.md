# Project3_CSci846 Hadoop Document likeness estimator. 

This is a java based application that uses hadoop meant to demonstrate the core concepts of distributated environment. The estimator application will take documents stored in a directory and determine how closely related pairs of the documents are based on their specified ngram size. 

## Getting Started

Pull down a local copy of the project.  

### Prerequisites

The system must have java installed.  
The system must have hadoop installed and setup.

```
java version 1.8.0_151 or greated is required
```

### Installing and Setup

Pull down a copy of the project. The project includes a bash script that will setup the environment variables, build the project, and create the jar. Just issue the following commands.

```
cd Project3_843/src/
chmod 755 est.sh 
./est.sh
```

Create an input directory on the HDFS.

```
hadoop fs -mkdir /user/yourName/estimator/input
```

Add txt files for input to the hadoop HDFS by issuing the following command for each file.

```
hadoop fs -copyFromLocal fileName /user/yourName/estimator/input/fileName
```


## Running the project

The application takes 4 parameters, however, only input and output are required. If not n or k values are set, each will default to the value 2:

1. 𝑛: an integer value greater than 0, determiniing the size of the n-grams
2. 𝑘: an integer value greater than 0, determiniing a threshold for which a couple of documents are regarded to contain simialrity. I.e. the algorithm will plot only couples with Sim(𝑑𝑜𝑐1 , 𝑑𝑜𝑐2) ≥ 𝑘
3. input: a directory containing documents to run the algorithm on
4. output: the name of a directory to store the output


```
hadoop jar est.jar Estimator -input /user/yourName/estimator/input -output /user/yourName/estimator/output
```

## Viewing the output

To view to output, run the following command to read back the results

```
hadoop fs -cat /user/yourName/estimator/output/part-r-00000
```

The output should show any document names in pairs and the number of matching ngrams that were found.  

```
file01, file02  5
file01, file03  2
file02, file03  2
```

## Built With

* [Java](https://www.oracle.com/technetwork/java/javase/downloads/index.html) - Language
* [Eclipse](https://www.eclipse.org/) - IDE 
* [Hadoop](https://hadoop.apache.org/) - Distributed Processing Framework

## Authors

* **Michael Moody** - *Initial work* - [dcmoods](https://github.com/dcmoods)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

