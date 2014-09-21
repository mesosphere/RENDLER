
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;
import com.google.protobuf.ByteString;

public class RendlerScheduler implements Scheduler {
    
    private final ExecutorInfo executorCrawler, executorRender;
	private final int totalTasks;
	private int launchedTasks = 0;
	private int finishedTasks = 0;
	private List<String> crawlQueue;
	private List<String> completedCrawlQueue;
	private Map<String,List<String>> edgeList;
    
	public RendlerScheduler(ExecutorInfo executorCrawler, ExecutorInfo executorRender) {
		this(executorCrawler, executorRender, 5);
	}
    
	public RendlerScheduler(ExecutorInfo executorCrawler, ExecutorInfo executorRender, int totalTasks) {
		this.executorCrawler = executorCrawler;
        this.executorRender = executorRender;
		this.totalTasks = totalTasks;
		this.crawlQueue = Collections.synchronizedList(new ArrayList<String>());
		this.completedCrawlQueue = Collections.synchronizedList(new ArrayList<String>());
		this.edgeList = Collections.synchronizedMap(new HashMap<String,List<String>>());
		this.crawlQueue.add("http://mesosphere.io/");
        
	}
    
	@Override
	public void registered(SchedulerDriver driver,
                           FrameworkID frameworkId,
                           MasterInfo masterInfo) {
		System.out.println("Registered! ID = " + frameworkId.getValue());
	}
    
	@Override
	public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {}
    
	@Override
	public void disconnected(SchedulerDriver driver) {}
    
	@Override
	public void resourceOffers(SchedulerDriver driver,
                               List<Offer> offers) {
		for (Offer offer : offers) {
			List<TaskInfo> tasks = new ArrayList<TaskInfo>();
			if (launchedTasks < totalTasks && !crawlQueue.isEmpty()) {
				TaskID taskId = TaskID.newBuilder()
                .setValue(Integer.toString(launchedTasks++)).build();
                
				System.out.println("Launching task " + taskId.getValue() + " with input: " + crawlQueue.get(0));
                //Task for crawler
				TaskInfo task = TaskInfo.newBuilder()
                .setName("task " + taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addResources(Resource.newBuilder()
                              .setName("cpus")
                              .setType(Value.Type.SCALAR)
                              .setScalar(Value.Scalar.newBuilder().setValue(1)))
                .addResources(Resource.newBuilder()
                              .setName("mem")
                              .setType(Value.Type.SCALAR)
                              .setScalar(Value.Scalar.newBuilder().setValue(128)))
                .setData(ByteString.copyFromUtf8(crawlQueue.get(0)))
                .setExecutor(ExecutorInfo.newBuilder(executorCrawler))
                .build();
                
                taskId = TaskID.newBuilder()
                .setValue(Integer.toString(launchedTasks++)).build();
                
                System.out.println("Launching task " + taskId.getValue() + " with input: " + crawlQueue.get(0));
                
                //Task for render
                TaskInfo taskRender = TaskInfo.newBuilder()
                .setName("task " + taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addResources(Resource.newBuilder()
                              .setName("cpus")
                              .setType(Value.Type.SCALAR)
                              .setScalar(Value.Scalar.newBuilder().setValue(1)))
                .addResources(Resource.newBuilder()
                              .setName("mem")
                              .setType(Value.Type.SCALAR)
                              .setScalar(Value.Scalar.newBuilder().setValue(128)))
                .setData(ByteString.copyFromUtf8(crawlQueue.get(0)))
                .setExecutor(ExecutorInfo.newBuilder(executorRender))
                .build();
                
				tasks.add(task);
                tasks.add(taskRender);
                completedCrawlQueue.add(crawlQueue.get(0));
				crawlQueue.remove(0);
                
			}
			driver.launchTasks(offer.getId(), tasks);
		}
	}
    
	@Override
	public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}
    
	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
		System.out.println("Status update: task " + status.getTaskId().getValue() +
                           " is in state " + status.getState());
		if (status.getState() == TaskState.TASK_FINISHED || status.getState() == TaskState.TASK_LOST) {
			finishedTasks++;
			System.out.println("Finished tasks: " + finishedTasks);
			if (finishedTasks == totalTasks) {
				driver.stop();
			}
		}
	}
    
	@Override
	public void frameworkMessage(SchedulerDriver driver,
                                 ExecutorID executorId,
                                 SlaveID slaveId,
                                 byte[] data) {
		String childUrlStr = new String(data);
        if (childUrlStr.startsWith("crawl")) {
            childUrlStr = childUrlStr.substring(5);
            //add all links found to the queue
            if (childUrlStr.length() >= 2) {
                childUrlStr = childUrlStr.substring(1, childUrlStr.length() -1);
                String[] arrListUrls = childUrlStr.split(",");
                LinkedList<String> listUrls = new LinkedList<String>(Arrays.asList(arrListUrls));
                for (int i = 0; i <listUrls.size(); i++) {
                    String listUrl = listUrls.get(i);
                    listUrl = listUrl.replaceAll("\\s+","");
                    if (!completedCrawlQueue.contains(listUrl) && !crawlQueue.contains(listUrl)) {
                        crawlQueue.add(listUrl);
                    }
                }
                //Create the edges from the source to all child links
                String sourceUrl = listUrls.get(0);
                edgeList.put(sourceUrl, listUrls);
            }
        }
	}
    
	@Override
	public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}
    
	@Override
	public void executorLost(SchedulerDriver driver,
                             ExecutorID executorId,
                             SlaveID slaveId,
                             int status) {}
    
	public void error(SchedulerDriver driver, String message) {
		System.out.println("Error: " + message);
	}
    
    
    
}

