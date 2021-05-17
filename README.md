# zipnshare

This is a web application that zips uploaded files and then shares them.

## Advantage

* Supports multiple file selection.
* Supports drag and drop file selection.
* Supports the creation of password protected zip files.
* Supports Local File System as a data storage destination.
* Supports Azure Blob Storage as a data storage destination.


## Prerequisite

* [JDK 8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
* [Apache Maven](https://maven.apache.org/download.cgi)


## How to use

To build jar
```
$ mvn clean package
```

To run web application
([http://localhost:8080/zipnshare/upload.html](http://localhost:8080/zipnshare/upload.html))

```
$ ./start.sh
```


## License

Apache License 2.0

