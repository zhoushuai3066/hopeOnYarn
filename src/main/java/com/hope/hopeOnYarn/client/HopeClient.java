package com.hope.hopeOnYarn.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.hope.hopeOnYarn.constants.AppConstants;
import com.hope.hopeOnYarn.utils.HopeHDFSUtils;

public class HopeClient {

	private static final Log LOG = LogFactory.getLog(HopeClient.class);

	private final long clientStartTime = System.currentTimeMillis();
	private long clientTimeout = 600000;
	private YarnClient yarnClient;
	private Options opts;
	private Configuration conf;

	private String appName = "BusapApp";
	
	/**
	 * applicationMaster��container��job��Ӧ�����伶��
	 */
	private int amPriority = 0;
	
	/**
	 * applicationMaster��container�ύ�Ķ���
	 */
	private String amQueue = "default";
	
	/**
	 * applicationMaster������Ҫ���ڴ�
	 */
	private int amMemory = 1000;


	/**
	 * applicationMaster������Ҫ��cpu��Դ
	 */
	private int amVCores = 2;
	
	/**
	 * container��Ҫ���ڴ���Դ
	 */
	private int containerMemory = 1000;
	
	/**
	 * container��Ҫ��cpu��Դ
	 */
	private int containerVirtualCores = 2;
	
	/**
	 * Ĭ��������container����������ֻ������һ��
	 */
	private int numContainers = 1;

	/** 
	 * ApplicationMasterJar�ı��ص�ַ(�����ַ)
	 */
	private String appMasterJar = ""; 
	
	
	/**
	 * ��Ҫ���е�jar�ĵ�ַ(�����ַ)
	 */
	private String appJar = "";
	
	private String mainClass = "";
	
	
	/**
	 * ��Ҫ���е�jar�ĵ�ַ(�����ַ)
	 */
	private String appJarPath = "runapp.jar";
	
	private String appLocalFiles;
	private String appHDFSFiles;
	
	
	/**
	 * hdfs�Ĺ�����
	 */
	private HopeHDFSUtils hdfsutils;
	
	
	
	/**
	 * applicationMaster����������
	 */
	private final String appMasterMainClass = "com.busap.applicationmaster.BusapApplicationMaster";
	
	
	/**
	 * applicationMaster��jar���ϴ���hdfs�������
	 */
	private static final String appMasterJarPath = "BusapApplicationMaster.jar";
	
	
	public HopeClient(){
		this(new Configuration());
	}


	public HopeClient(Configuration conf) {
		 	this.conf = conf;
		 	try {
		 		hdfsutils = HopeHDFSUtils.getInstances(this.conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    yarnClient = YarnClient.createYarnClient();
		    yarnClient.init(conf);
		    opts = new Options();
		    opts.addOption("appname", true, "Ӧ�õ���ƣ�Ĭ��ΪZhoushuaiApp");
		    opts.addOption("priority", true, "Ӧ�õ����ȼ���. Ĭ���� 0");
		    opts.addOption("queue", true, "Ӧ�����еĶ�����ƣ�Ĭ����default");
		    opts.addOption("master_memory", true, "applicationMaster�ڴ�����Ĭ����1000m");
		    opts.addOption("master_vcores", true, "applicationMastercpu����Ĭ����2vcpu");
		    opts.addOption("container_memory", true, "app���е��ڴ�����Ĭ����1000m");
		    opts.addOption("container_vcores", true, "app���е�cpu����Ĭ����2vcpu");
			opts.addOption("jar", true, "�û������jar��·��");
			opts.addOption("mainClass", true, "�Զ���jar���main·��(�������+����)");
			opts.addOption("amjar", true, "applicationMaster��Ӧ��jar(���ǵ�ǰ��yarn����)");
			opts.addOption("appLocalFiles", true, "�����Զ���jar��main���յ��ϴ����ļ���������main������˳�򴫵ݸ�jar�е�main���������ݵ���files�ı���·����ַ,���file�Ļ����Զ��ŷָ�");
			opts.addOption("appHDFSFiles", true, "�����Զ���jar��main���յ��ϴ����ļ���������main������˳�򴫵ݸ�jar�е�main���������ݵ���files�ı���·����ַ,���file�Ļ�����|�ָ�");
		 
	}

	
	/**
	 * ����1
	 * @param args �����е��������
	 * @return
	 * @throws ParseException
	 */
	public boolean init(String[] args) throws ParseException {
		
		
		CommandLine cliParser = new GnuParser().parse(opts, args);
//		LOG.info("����������ǣ�"+cliParser.toString());
		
		appName = cliParser.getOptionValue("appname", appName);
		amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		amQueue = cliParser.getOptionValue("queue", "default");
		amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory",String.valueOf(amMemory)));
		amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores",String.valueOf(amVCores)));
		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", String.valueOf(containerMemory)));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", String.valueOf(containerVirtualCores)));
		appLocalFiles =  cliParser.getOptionValue("appLocalFiles", "");
		appHDFSFiles = cliParser.getOptionValue("appHDFSFiles", "");
	    if (!cliParser.hasOption("amjar")) {
		        throw new IllegalArgumentException("��ָ��applicationMaster���ڵ�jar��·��(�����ַ)");
		      }		
	    
	    if (!cliParser.hasOption("jar")) {
	        throw new IllegalArgumentException("��ָ��Ҫִ�е�jar�������ڵİ�·��(�����ַ)");
	      }		
	    
	    if (!cliParser.hasOption("mainClass")) {
	        throw new IllegalArgumentException("��ָ��Ҫִ�е�jar�����main·��(�������+����)");
	      }		
	    
	    
        appMasterJar = cliParser.getOptionValue("amjar");
        
        appJar =  cliParser.getOptionValue("jar");
        
        mainClass = cliParser.getOptionValue("mainClass");
        
		return true;
	}
	
	
	public boolean run() throws IOException, YarnException {
		 yarnClient.start();
		 YarnClientApplication app = yarnClient.createApplication();
	     GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
	     int maxMem = appResponse.getMaximumResourceCapability().getMemory();
	     int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
	     ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
	     ApplicationId appId = appContext.getApplicationId();
	     appContext.setApplicationName(appName);
	     
	     
	     
	     ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
	     //�O�ñ����YԴ�����������YԴ�O�õ�container��
	     Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
	     
	     //��applicationMaster��jar���ϴ���hdfs
	     String applicationMasterHdfsPath =hdfsutils.getPath(appName, appId.toString(), appMasterJarPath);
	     Path applicationMasterPath =  hdfsutils.upload(appMasterJar, applicationMasterHdfsPath);
	     FileStatus applicationMasterFileStatus = hdfsutils.getFileStatus(applicationMasterPath);
	     LOG.info("�ϴ���applicationMasterHdfsPath:"+applicationMasterHdfsPath);
	     
		 LocalResource applicationMasterResource =LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(applicationMasterPath.toUri()),LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,applicationMasterFileStatus.getLen(), applicationMasterFileStatus.getModificationTime());
		 localResources.put(appMasterJarPath, applicationMasterResource);
		 
//	     addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.getId(),
//	    	        localResources, null);
		 
		 //���Զ����jar���ϴ���hdfs��
		 String appHdfsPath =hdfsutils.getPath(appName, appId.toString(), appJarPath);
	     Path appPath =  hdfsutils.upload(appJar, appHdfsPath);
	     FileStatus appFileStatus = hdfsutils.getFileStatus(appPath);
	     LOG.info("�ϴ���appHdfsPath:"+appHdfsPath);
	     
	     //��ָ���ı����ļ��ϴ���hdfs��
	     StringBuffer localfileinhdfspaths = new StringBuffer();
	     if(!this.appLocalFiles.equals("")){
	     String[] localfiles = this.appLocalFiles.split(",");
	     for(String f:localfiles){
	    	 String localfilePath =hdfsutils.getlocalFilePathinHDFS(appName, appId.toString(), f);
	    	 Path filePath =  hdfsutils.upload(f, localfilePath);
	    	 localfileinhdfspaths.append(filePath.toUri().toString());
	    	 localfileinhdfspaths.append(",");
	     }
	     LOG.info("�ϴ���localfilesPath:"+localfileinhdfspaths.toString());
	     }
	     
	     //��ָ����HDFS�ļ���ַ
	     StringBuffer hdfsfileinhdfspaths = new StringBuffer();
	     if(!this.appHDFSFiles.equals("")){
	    	 String[] hdfsfiles = this.appHDFSFiles.split(",");
	    	 for(String f:hdfsfiles){
	    		 Path filePath = hdfsutils.getHdfsPath(f);
	    		 hdfsfileinhdfspaths.append(filePath.toUri().toString());
	    		 hdfsfileinhdfspaths.append(",");
	    	 }
	    	 LOG.info("�ϴ���localfilesPath:"+hdfsfileinhdfspaths.toString());
	     }
	     
	     
	     amContainer.setLocalResources(localResources);
	     
	     
	     /**
	      * ��������applicationMaster��container�Ļ�������
	      */
	     Map<String, String> env = new HashMap<String, String>();
	     StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
	     for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
	       classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
	       classPathEnv.append(c.trim());
	     }
	     // add the runtime classpath needed for tests to work
	     if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
	       classPathEnv.append(':');
	       classPathEnv.append(System.getProperty("java.class.path"));
	     }
          
	     
	     LOG.info("ִ�еĻ�������:"+classPathEnv);
	     env.put("CLASSPATH", classPathEnv.toString());
	     env.put(AppConstants.APPLOCATION, appPath.toUri().toString());
	     env.put(AppConstants.APPTIMESTAMP, Long.toString(appFileStatus.getModificationTime()));
	     env.put(AppConstants.APPLEN, Long.toString(appFileStatus.getLen()));
	     env.put(AppConstants.APPMAINCLASS,this.mainClass);
	     env.put(AppConstants.APPLOCALFILES,localfileinhdfspaths.toString());
	     env.put(AppConstants.APPHDFSFILES,hdfsfileinhdfspaths.toString());
	     amContainer.setEnvironment(env);
	     
    	 /**
    	  * ��������applicationMaster��container�ĵ���������
    	  */
	     Vector<CharSequence> vargs = new Vector<CharSequence>(30);
	     vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
	     // Set Xmx based on am memory size
	     vargs.add("-Xmx" + amMemory + "m");
	     // Set class name 
	     vargs.add(appMasterMainClass);
	     // Set params for Application Master
	     vargs.add("--container_memory " + String.valueOf(containerMemory));
	     vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
	     vargs.add("--num_containers " + String.valueOf(numContainers));
	     vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
	     vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
	  // Get final commmand
	     StringBuilder command = new StringBuilder();
	     for (CharSequence str : vargs) {
	       command.append(str).append(" ");
	     }
	     List<String> commands = new ArrayList<String>();
	     commands.add(command.toString());		
	     
	     LOG.info(appMasterMainClass+"ִ�е�������:"+commands);
	     amContainer.setCommands(commands);
	     
	     
          /**
           * ���ô�resourcemanager��ȡ��Ҫ����Դ��
           */
	     // Set up resource type requirements
	     // For now, both memory and vcores are supported, so we set memory and 
	     // vcores requirements
	     Resource capability = Records.newRecord(Resource.class);
	     capability.setMemory(amMemory);
	     capability.setVirtualCores(amVCores);
	     appContext.setResource(capability);
	     
	     
         /**
          * ����appcontext��������Ҫ��container����
          */
	     appContext.setAMContainerSpec(amContainer);
	     
	     /**
	      * ����appcontext��������Ӧ�õ����ȼ�
	      */
	     Priority pri = Records.newRecord(Priority.class);
	     pri.setPriority(amPriority);
	     appContext.setPriority(pri);
	     
	     /**
	      * ����appconetxt��������Ӧ��ʹ�õĶ���
	      */
	     appContext.setQueue(amQueue);
	     
	     
	     /**
	      * �ύӦ�õ�yarn
	      */
	     yarnClient.submitApplication(appContext);
	     
	     
		return monitorApplication(appId);
	}
	
	
	
	
	/**
	 * Ӧ���ύ�Ժ����������һֱ���Ӧ�õ�״̬����Ӧ��ִ����ϻ���ʧ�ܲŻ����
	 * @param appId
	 * @return
	 * @throws YarnException
	 * @throws IOException
	 */
	  private boolean monitorApplication(ApplicationId appId)
	      throws YarnException, IOException {

	    while (true) {

	      // Check app status every 1 second.
	      try {
	        Thread.sleep(1000);
	      } catch (InterruptedException e) {
	      }

	      // Get application report for the appId we are interested in 
	      ApplicationReport report = yarnClient.getApplicationReport(appId);


	      YarnApplicationState state = report.getYarnApplicationState();
	      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
	      if (YarnApplicationState.FINISHED == state) {
	        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
	          return true;        
	        }
	        else {
	          return false;
	        }			  
	      }
	      else if (YarnApplicationState.KILLED == state	
	          || YarnApplicationState.FAILED == state) {
	        return false;
	      }			

	      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
	        forceKillApplication(appId);
	        return false;				
	      }
	    }			

	  }
	  
	  
	  /**
	   * Kill a submitted application by sending a call to the ASM
	   * @param appId Application Id to be killed. 
	   * @throws YarnException
	   * @throws IOException
	   */
	  private void forceKillApplication(ApplicationId appId)
	      throws YarnException, IOException {
	    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
	    // the same time. 
	    // If yes, can we kill a particular attempt only?

	    // Response can be ignored as it is non-null on success or 
	    // throws an exception in case of failures
	    yarnClient.killApplication(appId);	
	  }
	
	  
	/**
	 * ��������Դ�ϴ���hdfs�У�
	 * �÷�1������ǽ�������Դ�ϴ���hdfs�Ļ�����Ҫ����fileSrcPath��fileDstPath����resources��Ҫ����Ϊnull��
	 * �÷�2�����������hdfs������ļ������������fileSrcPathΪnull����������fileDstPath��resources��resourceΪҪ�������ļ�������
	 * @param fs
	 * @param fileSrcPath ������Դ��·��
	 * @param fileDstPath Ŀ��hdfs�е�Ŀ¼
	 * @param appId     Ӧ�õ�id
	 * @param localResources  �ϴ������Ҫ�����ϴ�����Դ����Ϣ�������Ҫ��localResources��Ӧ��mapͨ��ContainerLaunchContext#setLocalResources(localResources)�������õ�container��
	 * @param resources   //�����ļ����ļ����������ַ�
	 * @throws IOException
	 */
	private void addToLocalResources(FileSystem fs, String fileSrcPath,
		      String fileDstPath, int appId, Map<String, LocalResource> localResources,
		      String resources) throws IOException {
		    String suffix =appName + "/" + appId + "/" + fileDstPath;
		    Path dst =new Path(fs.getHomeDirectory(), suffix);
		    
		    if (fileSrcPath == null) {
		      FSDataOutputStream ostream = null;
		      try {
		        ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
		        ostream.writeUTF(resources);
		      } finally {
		        IOUtils.closeQuietly(ostream);
		      }
		    } else {
		      fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		    }
		    
		    
		    FileStatus scFileStatus = fs.getFileStatus(dst);
		    LocalResource scRsrc =
		        LocalResource.newInstance(
		            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
		            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
		            scFileStatus.getLen(), scFileStatus.getModificationTime());
		    localResources.put(fileDstPath, scRsrc);
		  }

	public static void main(String[] args) {
		
		HopeClient client = new HopeClient();
		try {
			client.init(args);
			client.run();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (YarnException e) {
			e.printStackTrace();
		}

	}

}
