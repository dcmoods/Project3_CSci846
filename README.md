# Project3_CSci846 Hadoop Document likeness estimator. 

This is a java based application that uses hadoop meant to demoonstrate the core concepts of distributated environment. The estimator application will take documents stored in a directory and determine how closely related pairs of the documents are based on their specified ngram size. 

## Getting Started

Pull down a local copy of the project.  

### Prerequisites

The system must have java installed.  
The system must have hadoop installed and setup.

```
java version 1.8.0_151 or greated is required
```

### Installing

The project includes a bash script that will setup the environment variables, build the project, and create the jar.

```
 ./est.sh
```

Repeat for the TCPPhone and TCPName Servers, as well as the TCPClient. The Client applicaiton must be last. 

```
Right Click TCPClient project -> "Run as" -> Java Application. 
```

In the command prompt for the client enter:
```
get phone alan
```

You should see a response of:
```
701-111-2222
```

## Running the projects from JAR files

If running the servers from the complied jar files, then open a command propmt, navigate to each folder and issue the following:

```
java -jar [FileName].jar
```
This must be done for each project, so a total of four command propmts must be opened. 

## The easy way

Double click the Batch file included to Project_Jar directory and all the command prompts will launched automatically. When finished just close the windows to terminate. 

## Built With

* [Java](https://www.oracle.com/technetwork/java/javase/downloads/index.html) - Language
* [Eclipse](https://www.eclipse.org/) - IDE 

## Authors

* **Michael Moody** - *Initial work* - [dcmoods](https://github.com/dcmoods)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

