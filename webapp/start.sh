#!/bin/sh

java -cp web/WEB-INF/classes:target/mitsuji-zipnshare-0.9.1-jar-with-dependencies.jar HttpService 0.0.0.0 8080 web
