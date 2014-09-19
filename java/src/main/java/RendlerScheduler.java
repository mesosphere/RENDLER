
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;
import com.google.protobuf.ByteString;

public class RendlerScheduler implements Scheduler {
	public RendlerScheduler(ExecutorInfo executor) {
		this(executor, 5);
	}
    
	public RendlerScheduler(ExecutorInfo executor, int totalTasks) {
		this.executor = executor;
        System.out.println("Executor" + executor.getName());
		this.totalTasks = totalTasks;
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
			if (launchedTasks < totalTasks) {
				TaskID taskId = TaskID.newBuilder()
                .setValue(Integer.toString(launchedTasks++)).build();
                
				System.out.println("Launching task " + taskId.getValue());
                
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
                .setData(ByteString.copyFromUtf8("http://mesosphere.io/"))
                .setExecutor(ExecutorInfo.newBuilder(executor))
                .build();
				tasks.add(task);
			}
			Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();
			driver.launchTasks(offer.getId(), tasks, filters);
		}
	}
    
	@Override
	public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}
    
	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
		System.out.println("Status update: task " + status.getTaskId().getValue() +
                           " is in state " + status.getState());
		if (status.getState() == TaskState.TASK_FINISHED) {
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
        System.out.println(new String(data));
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
    
	private final ExecutorInfo executor;
	private final int totalTasks;
	private int launchedTasks = 0;
	private int finishedTasks = 0;

}

