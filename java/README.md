Java Rendler Framework:

1) cd hostfiles/java
2) mvn clean compile assembly:single
3) export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
4) java -cp target/rendler-1.0-SNAPSHOT-jar-with-dependencies.jar RendlerMain 127.0.1.1:5050 250
5) ../bin/make-pdf

