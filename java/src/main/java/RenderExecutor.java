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
        
		String url = pTaskInfo.getData().toString();
        String currPath = System.getProperty("user.dir");
        String renderJSPath =  currPath + "/render.js";
        String workPathDir = currPath + "/rendler-work-dir/";
        
        //run phantom js
        String filename = workPathDir + pTaskInfo.getTaskId().toString() + ".png";
        String cmd = "phantomjs  "+ renderJSPath + " " + url + " " + filename;
        try {
            Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            System.out.println("Exception executing phantomjs: " + e);
        }
       
        //send framework message and set task to finished
        String myStatus = pTaskInfo.getTaskId() + url + filename;
		pDriver.sendFrameworkMessage(myStatus.getBytes());
        status = TaskStatus.newBuilder()
        .setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_FINISHED).build();
		pDriver.sendStatusUpdate(status);
        
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
