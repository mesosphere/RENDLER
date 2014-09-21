import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;


public class RenderExecutor implements Executor {
    
    @Override
    public void registered(ExecutorDriver driver,
                           ExecutorInfo executorInfo,
                           FrameworkInfo frameworkInfo,
                           SlaveInfo slaveInfo) {
        System.out.println("Registered executor on " + slaveInfo.getHostname());
    }
    
    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo) {}
    
    @Override
    public void disconnected(ExecutorDriver driver) {}
    
    @Override
	public void launchTask(ExecutorDriver pDriver, TaskInfo pTaskInfo) {
        //start task with status running
        TaskStatus status = TaskStatus.newBuilder()
        .setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING).build();
        pDriver.sendStatusUpdate(status);
        
		String url = pTaskInfo.getData().toStringUtf8();
        //TODO remove hard coding
        String currPath = "/home/vagrant/sandbox/mesosphere/mesos-sdk/RENDLER/java";
        String renderJSPath =  currPath + "/render.js";
        String workPathDir = currPath + "/rendleroutput/";
        //run phantom js
        String filename = workPathDir + pTaskInfo.getTaskId().getValue() + ".png";
        String cmd = "phantomjs " + renderJSPath + " " + url + " " + filename;
        
        try {
            runProcess(cmd);
            //if successful send the framework message
            if (new File(filename).exists()) {
                String myStatus = url + "," + filename;
                pDriver.sendFrameworkMessage(myStatus.getBytes());
            }
            
        } catch (Exception e) {
            System.out.println("Exception executing phantomjs: " + e);
        }
        
        
        //set the task to finished
        status = TaskStatus.newBuilder()
        .setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_FINISHED).build();
		pDriver.sendStatusUpdate(status);
        
	}
    
    private void printLines(String name, InputStream ins) throws Exception {
        String line = null;
        BufferedReader in = new BufferedReader(
                                               new InputStreamReader(ins));
        while ((line = in.readLine()) != null) {
            System.out.println(name + " " + line);
        }
    }
    
    private void runProcess(String command) throws Exception {
        Process pro = Runtime.getRuntime().exec(command);
        printLines(command + " stdout:", pro.getInputStream());
        printLines(command + " stderr:", pro.getErrorStream());
        pro.waitFor();
        System.out.println(command + " exitValue() " + pro.exitValue());
    }
    
    @Override
    public void killTask(ExecutorDriver driver, TaskID taskId) {}
    
    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {}
    
    @Override
    public void shutdown(ExecutorDriver driver) {}
    
    @Override
    public void error(ExecutorDriver driver, String message) {}
    
    public static void main(String[] args) throws Exception {
        MesosExecutorDriver driver = new MesosExecutorDriver(new RenderExecutor());
        System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }
    
    
}
