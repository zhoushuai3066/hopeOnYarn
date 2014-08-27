package com.hope.hopeOnYarn.containerRunnable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.hope.hopeOnYarn.amnm.HopeNMCallbackHandler;
import com.hope.hopeOnYarn.model.AppInfo;
import com.hope.hopeOnYarn.utils.HopeHDFSUtils;

public class HopeLaunchContainerRunnable implements Runnable{
	
	 private static final Log LOG = LogFactory.getLog(HopeLaunchContainerRunnable.class);
	
	 // Allocated container
    Container container;

    HopeNMCallbackHandler containerListener;
    
    private NMClientAsync nmClientAsync;
    
    private Configuration conf;
    
    private AppInfo appinfo;
    
    /**
     * @param lcontainer Allocated container
     * @param containerListener Callback handler of the container
     */
    public HopeLaunchContainerRunnable(AppInfo appinfo,Container lcontainer, HopeNMCallbackHandler containerListener,Configuration conf,NMClientAsync nmClientAsync) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.conf = conf;
      this.nmClientAsync = nmClientAsync;
      this.appinfo = appinfo;
    }

	public void run() {
		      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		      
	         /**
	          * �������л�������
	          */
		     Map<String, String> env = new HashMap<String, String>();
		     StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		     classPathEnv.append(Environment.APP_CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./runapp.jar/lib/*");
//		     Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./runapp/lib/*");
//		     Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./runapp.jar/lib/*");
		     for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
		       classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
		       classPathEnv.append(c.trim());
		     }
		     if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
		       classPathEnv.append(':');
		       classPathEnv.append(System.getProperty("java.class.path"));
		     }
		     LOG.info("Appִ�еĻ�������:"+classPathEnv.toString());
		     
		     env.put("CLASSPATH", classPathEnv.toString());
		     ctx.setEnvironment(env);
			     
		     /**
		      * ����������Ҫ����Դ
		      */
		      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		      StringBuffer localfilessb = new StringBuffer();
		      StringBuffer hdfsfilessb = new StringBuffer();
		      try {
				FileSystem fs = FileSystem.get(conf);
				Path dst = new Path(fs.getHomeDirectory(),appinfo.getApplocation());
				 LOG.info("appinfo.getApplocation():"+appinfo.getApplocation());
				URL url = ConverterUtils.getYarnUrlFromURI(dst.toUri());
				long len = new Long(appinfo.getApplen());
				long timestamp = new Long(appinfo.getApptimestamp());
			    LocalResource scRsrc =LocalResource.newInstance(url,LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,len, timestamp);
			    localResources.put("runapp.jar", scRsrc);
			    
			    if(!appinfo.getAppLocalFiles().equals("")){
			    	  String[] localfiles = appinfo.getAppLocalFiles().split(",");
			 	     for(String f:localfiles){
			 	    	  Path bdst = new Path(fs.getHomeDirectory(),f);
			 	    	  String name = new Date().getTime()+bdst.getName();
		 	    		  URL burl = ConverterUtils.getYarnUrlFromURI(bdst.toUri());
						  FileStatus localFileStatus = fs.getFileStatus(bdst);
						  LocalResource scRsrcfile =LocalResource.newInstance(burl,LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,localFileStatus.getLen(), localFileStatus.getModificationTime());
					      localResources.put(name, scRsrcfile);
					      localfilessb.append("./"+name);
					      localfilessb.append(",");
			 	     }
			 	     LOG.info("�ϴ���localfilesPath:"+localfilessb.toString());
			    }
			    
			    if(!appinfo.getAppHDFSFiles().equals("")){
			    	  String[] hdfsfiles = appinfo.getAppHDFSFiles().split(",");
			 	     for(String f:hdfsfiles){
			 	    	  Path bdst = new Path(fs.getHomeDirectory(),f);
			 	    	 String name = new Date().getTime()+bdst.getName();
		 	    		  URL burl = ConverterUtils.getYarnUrlFromURI(bdst.toUri());
						  FileStatus hdfsFileStatus = fs.getFileStatus(bdst);
						  LocalResource scRsrcfile =LocalResource.newInstance(burl,LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,hdfsFileStatus.getLen(), hdfsFileStatus.getModificationTime());
					      localResources.put(name, scRsrcfile);
					      hdfsfilessb.append("./"+name);
					      hdfsfilessb.append(",");
			 	     }
			 	     LOG.info("�ϴ���hdfsfilesPath:"+hdfsfilessb.toString());
			    }
			   
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		      ctx.setLocalResources(localResources);

		      
		      
		      Vector<CharSequence> vargs = new Vector<CharSequence>(30);

		      vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		      vargs.add("-Xmx1000m");
		      vargs.add("-Dlocalfiles=" + localfilessb.toString() + " ");
		      vargs.add("-Dhdfsfiles=" + hdfsfilessb.toString() + " ");
		      vargs.add(appinfo.getMainClass() + " ");
		      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
		      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

		      StringBuilder command = new StringBuilder();
		      for (CharSequence str : vargs) {
		        command.append(str).append(" ");
		      }
		      List<String> commands = new ArrayList<String>();
		      commands.add(command.toString());
		      ctx.setCommands(commands);
		      
		      
		      LOG.info("ִ�е�������:"+commands);
		      

		      nmClientAsync.startContainerAsync(container, ctx);
		      
		      
		
	}

}
