package com.hope.hopeOnYarn.amrm;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import com.hope.hopeOnYarn.amnm.HopeNMCallbackHandler;
import com.hope.hopeOnYarn.applicationmaster.HopeApplicationMaster;
import com.hope.hopeOnYarn.containerRunnable.HopeLaunchContainerRunnable;
import com.hope.hopeOnYarn.model.AppInfo;

public class HopeRMCallbackHandler implements AMRMClientAsync.CallbackHandler{
	
	private static final Log LOG = LogFactory.getLog(HopeRMCallbackHandler.class);
	  private HopeNMCallbackHandler containerListener;
	  
	  private List<Thread> launchThreads;
	  
	  private NMClientAsync nmClientAsync;
	    
      private Configuration conf;
      
      private AppInfo appinfo;
	  
    public HopeRMCallbackHandler(AppInfo appinfo,HopeNMCallbackHandler containerListener,List<Thread> launchThreads,NMClientAsync nmClientAsync,Configuration conf){
    	this.containerListener = containerListener;
    	this.launchThreads = launchThreads;
    	this.nmClientAsync = nmClientAsync;
    	this.conf = conf;
    	this.appinfo = appinfo;
    }
      
      
	public float getProgress() {
	      float progress = (float) 1/ 1;
	      return progress;
	}

	public void onContainersAllocated(List<Container> allocatedContainers) {
	      for (Container allocatedContainer : allocatedContainers) {
	    	  HopeLaunchContainerRunnable runnableLaunchContainer = new HopeLaunchContainerRunnable(appinfo,allocatedContainer, containerListener,conf,nmClientAsync);
	        Thread launchThread = new Thread(runnableLaunchContainer);
	        launchThreads.add(launchThread);
	        launchThread.start();
	      }
		
	}

	public void onContainersCompleted(List<ContainerStatus> completedContainers) {
		LOG.info("RM------onContainersCompleted");
	    HopeApplicationMaster.done = true;
	}

	public void onError(Throwable arg0) {
		LOG.info("RM------onError");
		HopeApplicationMaster.done = true;
	}

	public void onNodesUpdated(List<NodeReport> arg0) {
		LOG.info("RM------onNodesUpdated");
	}

	public void onShutdownRequest() {
		LOG.info("RM------onShutdownRequest");
	    HopeApplicationMaster.done = true;
	}
	
	

}
