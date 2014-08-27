package com.hope.hopeOnYarn.amnm;

import java.nio.ByteBuffer;
import java.util.Map;



import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import com.hope.hopeOnYarn.applicationmaster.HopeApplicationMaster;

public class HopeNMCallbackHandler implements NMClientAsync.CallbackHandler{
	
	private final HopeApplicationMaster applicationMaster;
	
	 public HopeNMCallbackHandler(HopeApplicationMaster zhoushuaiApplicationMaster) {
	      this.applicationMaster = zhoushuaiApplicationMaster;
	    }

	public void onContainerStarted(ContainerId arg0,
			Map<String, ByteBuffer> arg1) {
	}

	public void onContainerStatusReceived(ContainerId arg0, ContainerStatus arg1) {
		System.out.println("NM===================onContainerStatusReceived");
		
	}

	public void onContainerStopped(ContainerId arg0) {
		System.out.println("NM===================onContainerStopped");
		
	}

	public void onGetContainerStatusError(ContainerId arg0, Throwable arg1) {
		System.out.println("NM===================onGetContainerStatusError");
		
	}

	public void onStartContainerError(ContainerId arg0, Throwable arg1) {
		System.out.println("NM===================onStartContainerError");
		
	}

	public void onStopContainerError(ContainerId arg0, Throwable arg1) {
		System.out.println("NM===================onStopContainerError");
		
	}

}
