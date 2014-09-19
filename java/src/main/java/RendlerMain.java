
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;
import com.google.protobuf.ByteString;

public class RendlerMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            usage();
            System.exit(1);
        }
        
        String command = "java -cp rendler-1.0-SNAPSHOT-jar-with-dependencies.jar CrawlExecutor";
        String path = "/home/vagrant/sandbox/mesosphere/mesos-sdk/RENDLER/java/target/rendler-1.0-SNAPSHOT-jar-with-dependencies.jar";
        
        
        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(false).build();
        
        CommandInfo commandInfo = CommandInfo.newBuilder().setValue(command).addUris(uri).build();
        
        
        ExecutorInfo executor = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue("CrawlExecutor"))
        .setCommand(commandInfo)
        .setName("Crawl Executor (Java)")
        .setSource("java")
        .build();
        
        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("Test Framework (Java)");
        
        // TODO(vinod): Make checkpointing the default when it is default
        // on the slave.
        if (System.getenv("MESOS_CHECKPOINT") != null) {
            System.out.println("Enabling checkpoint for the framework");
            frameworkBuilder.setCheckpoint(true);
        }
        
        
        Scheduler scheduler = args.length == 1
        ? new RendlerScheduler(executor)
        : new RendlerScheduler(executor, Integer.parseInt(args[1]));
        
        MesosSchedulerDriver driver = null;
        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            System.out.println("Enabling authentication for the framework");
            
            if (System.getenv("DEFAULT_PRINCIPAL") == null) {
                System.err.println("Expecting authentication principal in the environment");
                System.exit(1);
            }
            
            if (System.getenv("DEFAULT_SECRET") == null) {
                System.err.println("Expecting authentication secret in the environment");
                System.exit(1);
            }
            
            Credential credential = Credential.newBuilder()
            .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
            .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()))
            .build();
            
            frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));
            
            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);
        } else {
            frameworkBuilder.setPrincipal("test-framework-java");
            
            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
        }
        
        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;
        
        // Ensure that the driver process terminates.
        driver.stop();
        
        System.exit(status);
    }
    
    private static void usage() {
        String name = RendlerScheduler.class.getName();
        System.err.println("Usage: " + name + " master <tasks>");
    }

}

