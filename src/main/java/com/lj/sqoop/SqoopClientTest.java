package com.lj.sqoop;


import java.util.List;
import java.util.ResourceBundle;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;

/**
 * 这个类只需要sqoop client的jar包即可
 * 参考网址
 * http://sqoop.apache.org/docs/1.99.6/ClientAPI.html
 * @author Administrator
 *
 */
public class SqoopClientTest {
	public static void main(String[] args){
		//初始化,这个应该是到服务器的url
		String url="http://172.16.135.160:duankou/sqoop/";
		SqoopClient client = new SqoopClient(url);
		//客户端的url可以通过如下设置修改
		//client.setServerUrl(newurl);
		
		//创建到客户端的连接
		long connectorId = 1;
		MLink link = client.createLink(connectorId);
		
		link.setName("Vampire");
		link.setCreationUser("Buffy");
		//设置连接的配置值
		MLinkConfig linkConfig = link.getConnectorLinkConfig();
		
		linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql");
		linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.mysql");
		linkConfig.getStringInput("linkConfig.username").setValue("root");
		linkConfig.getStringInput("linkConfig.password").setValue("123456");
		//保存配置好的link对象到client
		//一旦成功保存了link，link就会被分配新的id，这个id在sqoop的仓库中能保存唯一性
		Status status = client.saveLink(link);
		if(status.canProceed()){
			System.out.println("创建连接Link Id："+link.getPersistenceId());
		}else{
			System.out.println("创建连接失败");
		}
		
		//可以通过如下方法返回一个link
		//client.getLink(linkId);
		//如下方法返回的是client中的所有link连接
		//client.getLinks();
		//当我们创建一个job的from和to部分转移数据时，job需要制定from的linkid和to的linkid
		//所以创建连接时，需要先创建from和to的link链接
		
	}
	
	
	public  void createAndSaveJob(){
		String url="http://localhost:12000/sqoop";
		SqoopClient client = new SqoopClient(url);
		//创建连接
		long fromLinkId = 1;//for jdbc connector
		long toLinkId = 2;//for HDFS connector
		MJob job = client.createJob(fromLinkId, toLinkId);
		job.setName("Vampire");
		job.setCreationUser("Buffy");
		//设置from link的job配置值
		MFromConfig fromJobcomfig = job.getFromJobConfig();
		fromJobcomfig.getStringInput("fromJobConfig.schemaName").setValue("sqoop");
		fromJobcomfig.getStringInput("fromJobConfig.tableName").setValue("sqoop");
		//设置区分列
		fromJobcomfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
		// set the "TO" link job config values
		MToConfig toJobConfig = job.getToJobConfig();
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/usr/tmp");
		// set the driver config values
		MDriverConfig driverConfig = job.getDriverConfig();
		driverConfig.getStringInput("throttlingConfig.numExtractors").setValue("3");
		
		Status status = client.saveJob(job);
		if(status.canProceed()) {
		 System.out.println("Created Job with Job Id: "+ job.getPersistenceId());
		} else {
		 System.out.println("Something went wrong creating the job");
		}
		
		
	}
	
	
	
	
	
	
	/**
	 * 以下这个方法用来打印WARNing和ERROR的错误信息
	 * 使用方法如下：
	 * printMessage(link.getConnectorLinkConfig().getConfigs());
	 * @param configs
	 */
	public static void printMessage(List<MConfig> configs) {
		  for(MConfig config : configs) {
		    List<MInput<?>> inputlist = config.getInputs();
		    if (config.getValidationMessages() != null) {
		     // print every validation message
		     for(Message message : config.getValidationMessages()) {
		      System.out.println("Config validation message: " + message.getMessage());
		     }
		    }
		    for (MInput<?> minput : inputlist) {
		      if (minput.getValidationStatus() == Status.WARNING) {
		       for(Message message : config.getValidationMessages()) {
		        System.out.println("Config Input Validation Warning: " + message.getMessage());
		      }
		    }
		    else if (minput.getValidationStatus() == Status.ERROR) {
		      for(Message message : config.getValidationMessages()) {
		       System.out.println("Config Input Validation Error: " + message.getMessage());
		      }
		     }
		    }
		   }
	}
	
	
	
	public void startJob(long jobId,SqoopClient client){
		MSubmission submission = client.startJob(jobId);
		System.out.println("Job Submission Status : " + submission.getStatus());
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
		  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		System.out.println("Hadoop job id :" + submission.getExternalJobId());
		System.out.println("Job link : " + submission.getExternalLink());
		Counters counters = submission.getCounters();
		if(counters != null) {
		  System.out.println("Counters:");
		  for(CounterGroup group : counters) {
		    System.out.print("\t");
		    System.out.println(group.getName());
		    for(Counter counter : group) {
		      System.out.print("\t\t");
		      System.out.print(counter.getName());
		      System.out.print(": ");
		      System.out.println(counter.getValue());
		    }
		  }
		}
		if(submission.getError()!= null) {
		  System.out.println("Error info : " +submission.getError());
		}


		

		
		
	}
	
	/**
	 * 检查一个job的状态
	 * @param jobId
	 * @param client
	 */
	public void checkJobStatus(long jobId,SqoopClient client){
		//Check job status for a running job
				MSubmission submission = client.getJobStatus(jobId);
				if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
				  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
				}
	}
	public void stopJob(long jobId,SqoopClient client){
	
		client.stopJob(jobId);
		
	}
	
	
	public void displayConfigForConnector(Long connectorId,SqoopClient client){
		
		// link config for connector
		describe(client.getConnector(connectorId).getLinkConfig().getConfigs(), client.getConnectorConfigBundle(connectorId));
		// from job config for connector
		describe(client.getConnector(connectorId).getFromConfig().getConfigs(), client.getConnectorConfigBundle(connectorId));
		// to job config for the connector
		describe(client.getConnector(connectorId).getToConfig().getConfigs(), client.getConnectorConfigBundle(connectorId));
	}
	
	
	void describe(List<MConfig> configs, ResourceBundle resource) {
		  for (MConfig config : configs) {
		    System.out.println(resource.getString(config.getLabelKey())+":");
		    List<MInput<?>> inputs = config.getInputs();
		    for (MInput<?> input : inputs) {
		      System.out.println(resource.getString(input.getLabelKey()) + " : " + input.getValue());
		    }
		    System.out.println();
		  }
		}
	

}
