# Java Rendler Framework

From this directory:

1. `mvn clean compile assembly:single`
1. `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib`
1. `java -cp target/rendler-1.0-SNAPSHOT-jar-with-dependencies.jar RendlerMain 127.0.1.1:5050 250`
1. `../bin/make-pdf`
