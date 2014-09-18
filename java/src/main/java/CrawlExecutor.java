import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;

public class CrawlExecutor implements Executor {
    
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
       	
		try
		{
            //Parse the links from the url
            String urlData = getUrlSource(url);
            Pattern linkPattern = Pattern.compile("(<a[^>]+>.+?</a>)",  Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
            Matcher pageMatcher = linkPattern.matcher(urlData);
            ArrayList<String> links = new ArrayList<String>();
            while(pageMatcher.find()){
                links.add(pageMatcher.group());
            }
            // Write list of links to byte array
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dataoutputstream = new DataOutputStream(baos);
			for (String link : links) {
				dataoutputstream.writeUTF(link);
			}
			byte[] byteArrayLinks = baos.toByteArray();
			pDriver.sendFrameworkMessage(byteArrayLinks);
		}
		catch (IOException e) {
			System.out.println("Exception writing links to byte array: "+ e);
		}
        
        //send framework message and set task to finished
        status = TaskStatus.newBuilder()
        .setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_FINISHED).build();
		pDriver.sendStatusUpdate(status);
        
	}
    
    /**
     *  Exact the html source code from a webpage.
     *  @param pURL the page url
     *  @return the source code
     *
     **/
	private String getUrlSource(String pURL) throws IOException {
		URL siteURL = new URL(pURL);
		URLConnection connection = siteURL.openConnection();
		BufferedReader myBufReader = new BufferedReader(new InputStreamReader(
                                                                              connection.getInputStream(), "UTF-8"));
		String inputLine;
		StringBuilder mySB = new StringBuilder();
		while ((inputLine = myBufReader.readLine()) != null)
			mySB.append(inputLine);
		myBufReader.close();
        
		return mySB.toString();
	}
    
    @Override
    public void killTask(ExecutorDriver driver, TaskID taskId) {}
    
    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {}
    
    @Override
    public void shutdown(ExecutorDriver driver) {}
    
    @Override
    public void error(ExecutorDriver driver, String message) {}
    
}
