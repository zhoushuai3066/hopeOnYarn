package com.hope.hopeOnYarn.applicationmaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.api.records.Priority;

import com.hope.hopeOnYarn.amnm.HopeNMCallbackHandler;
import com.hope.hopeOnYarn.amrm.HopeRMCallbackHandler;
import com.hope.hopeOnYarn.constants.AppConstants;
import com.hope.hopeOnYarn.model.AppInfo;



public class HopeApplicationMaster {
	
	private static final Log LOG = LogFactory.getLog(HopeApplicationMaster.class);
	
	  protected ApplicationAttemptId appAttemptID;
	// App Master configuration
	  // No. of containers to run shell command on
	  private int numTotalContainers = 1;
	  // Memory to request for the container on which the shell command will run
	  private int containerMemory = 10;
	  // VirtualCores to request for the container on which the shell command will run
	  private int containerVirtualCores = 1;
	  // Priority of the request
	  private int requestPriority;
	  
	  @SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;
	  
	  private NMClientAsync nmClientAsync;
	  private HopeNMCallbackHandler containerListener;
	  
	  
	  private String appMasterHostname = "";
	  private int appMasterRpcPort = -1;
	  private String appMasterTrackingUrl = "";
	  

	  // Configuration
	  private Configuration conf;
	  
	  public static volatile boolean done;
	  
	  
	  private AppInfo appinfo = null;
	  
	  // Launch threads
	  private List<Thread> launchThreads = new ArrayList<Thread>();
	  
	  public HopeApplicationMaster(){
		  conf = new Configuration();
	  }
	
	  
	  /**
	   * ���������Ҫ�Ǵ��������л�ȡ��Ҫ��resourcemanager�������Դ��
	   * @param args
	   * @return
	   * @throws ParseException
	   * @throws IOException
	   */
	public boolean init(String[] args) throws ParseException, IOException {
		
		    Options opts = new Options();
		    opts.addOption("app_attempt_id", true,
		        "App Attempt ID. Not to be used unless for testing purposes");
		    
		    opts.addOption("container_memory", true,
		        "Amount of memory in MB to be requested to run the shell command");
		    
		    opts.addOption("container_vcores", true,
		        "Amount of virtual cores to be requested to run the shell command");
		    
		    opts.addOption("num_containers", true,
		        "No. of containers on which the shell command needs to be executed");
		    
		    opts.addOption("priority", true, "Application Priority. Default 0");
		    
		    opts.addOption("debug", false, "Dump out debug information");

		    opts.addOption("help", false, "Print usage");
		    
		    CommandLine cliParser = new GnuParser().parse(opts, args);
		    
		    /**
		     * ����app_attempt_id
		     */
		    Map<String, String> envs = System.getenv();
		    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
		        if (cliParser.hasOption("app_attempt_id")) {
		          String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
		          appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
		        } else {
		          throw new IllegalArgumentException(
		              "Application Attempt Id not set in the environment");
		        }
		      } else {
		        ContainerId containerId = ConverterUtils.toContainerId(envs
		            .get(Environment.CONTAINER_ID.name()));
		        appAttemptID = containerId.getApplicationAttemptId();
		      }
		    
		    
		    /**
		     * ����appjar��·������Ϣ
		     */
		    appinfo = new AppInfo();
		    if (envs.containsKey(AppConstants.APPLOCATION)) {
		    	appinfo.setApplocation(envs.get(AppConstants.APPLOCATION)); 
		      } 
		    
		    if (envs.containsKey(AppConstants.APPLEN)) {
		    	appinfo.setApplen(envs.get(AppConstants.APPLEN));
		      } 
		    
		    if (envs.containsKey(AppConstants.APPTIMESTAMP)) {
		    	appinfo.setApptimestamp(envs.get(AppConstants.APPTIMESTAMP));
		      } 
		    
		    if (envs.containsKey(AppConstants.APPMAINCLASS)) {
		    	appinfo.setMainClass(envs.get(AppConstants.APPMAINCLASS));
		      } 
		    
		    if (envs.containsKey(AppConstants.APPLOCALFILES)) {
		    	appinfo.setAppLocalFiles(envs.get(AppConstants.APPLOCALFILES));
		      } 
		    
		    if (envs.containsKey(AppConstants.APPHDFSFILES)) {
		    	appinfo.setAppHDFSFiles(envs.get(AppConstants.APPHDFSFILES));
		      } 
		      LOG.info("app �����л���:");
		      LOG.info(appinfo.getApplocation());
		      LOG.info(appinfo.getApplen());
		      LOG.info(appinfo.getApptimestamp());
		      LOG.info(appinfo.getMainClass());
		      LOG.info(appinfo.getAppLocalFiles());
		      LOG.info(appinfo.getAppHDFSFiles());
		    
		    /**
		     * ��������appAttempt����Դ����
		     */
		    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "1000"));
	        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "2"));
	        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
	        
	        /**
	         * ��������appAttempt�����ȼ�
	         */
	        requestPriority = Integer.parseInt(cliParser
	                .getOptionValue("priority", "0"));

		return true;
	}
	
	  
	  
	  public HopeNMCallbackHandler createNMCallbackHandler() {
		    return new HopeNMCallbackHandler(this);
		  }
	  
	  
	  /**
	   * Setup the request that will be sent to the RM for the container ask.
	   *
	   * @return the setup ResourceRequest to be sent to RM
	   */
	  private ContainerRequest setupContainerAskForRM() {
	    Priority pri = Records.newRecord(Priority.class);
	    pri.setPriority(requestPriority);
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(containerMemory);
	    capability.setVirtualCores(containerVirtualCores);
	    ContainerRequest request = new ContainerRequest(capability, null, null,pri);
	    return request;
	  }
	
	  public void run() throws YarnException, IOException {
		  
		  
		    containerListener = createNMCallbackHandler();
		    nmClientAsync = new NMClientAsyncImpl(containerListener);
		    
		    AMRMClientAsync.CallbackHandler allocListener = new HopeRMCallbackHandler(appinfo,containerListener,launchThreads,nmClientAsync,conf);
		    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		    amRMClient.init(conf);
		    amRMClient.start();
		    
		    nmClientAsync.init(conf);
		    nmClientAsync.start();
		    
		    appMasterHostname = NetUtils.getHostname();
		    
		    RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort,appMasterTrackingUrl);
		    
	        ContainerRequest containerAsk = setupContainerAskForRM();
	        amRMClient.addContainerRequest(containerAsk);
		  
	  }
	
	  
	  protected boolean finish() {
		    // wait for completion.
		    while (!done) {
		      try {
		        Thread.sleep(200);
		      } catch (InterruptedException ex) {}
		    }

		    for (Thread launchThread : launchThreads) {
		      try {
		        launchThread.join(10000);
		      } catch (InterruptedException e) {
		        e.printStackTrace();
		      }
		    }

		    
		    nmClientAsync.stop();


		    FinalApplicationStatus appStatus =  FinalApplicationStatus.SUCCEEDED;
		    try {
		      amRMClient.unregisterApplicationMaster(appStatus, "���", null);
		    } catch (YarnException ex) {
		    } catch (IOException e) {
		    }
		    amRMClient.stop();

		    return true;
		  }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		boolean result = false;
		HopeApplicationMaster appMaster = new HopeApplicationMaster();
		 try {
			boolean doRun = appMaster.init(args);
			 appMaster.run();
			 
			 result = appMaster.finish();
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		} catch (YarnException e) {
			e.printStackTrace();
		}
		 
		  if (result) {
		      System.exit(0);
		    } else {
		      System.exit(2);
		    }
	}

}
