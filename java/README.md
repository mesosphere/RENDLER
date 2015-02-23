Java Rendler Framework:

<br> 1) cd hostfiles/java </br> 
<br> 2) mvn clean compile assembly:single </br>
<br> 3) export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib </br>
<br> 4) java -cp target/rendler-1.0-SNAPSHOT-jar-with-dependencies.jar RendlerMain 127.0.1.1:5050 250 </br>
<br> 5) ../bin/make-pdf </br>

