# zipnshare

This is a web application that zips uploaded files and then shares them.

## Advantage

* Supports multiple file selection.
* Supports drag and drop file selection.
* Supports Local File System as a data storage destination.
* Supports Azure Blob Storage as a data storage destination.
* Supports AWS S3 Storage as a data storage destination.
* Supports the creation of ziped version of shared files.


## Prerequisite

* [JDK 8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
* [Apache Maven](https://maven.apache.org/download.cgi)


## How to use

To build jar
```
$ cd webapp
$ mvn clean package
```

To run web application
([http://localhost:8080/zipnshare/](http://localhost:8080/zipnshare/))

```
$ ./start.sh
```


## License

Apache License 2.0

