package com.hope.hopeOnYarn.utils;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HopeHDFSUtils {
	
	
	public static HopeHDFSUtils instances = null;
	
	
	private static FileSystem fs;
	
	
	private HopeHDFSUtils() {}
	
	
	
	

	public static HopeHDFSUtils getInstances(Configuration conf) throws IOException{
		
	       if(instances == null){
	    	   fs = FileSystem.get(conf);
	    	   instances = new HopeHDFSUtils();
	       }
	       
	       return instances;
	}
	
	/**
	 * ��������Դ�ϴ���hdfs�������hdfs
	 * @param fs
	 * @param fileSrcPath
	 * @param fileDstPath
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public  Path upload(String fileSrcPath,String fileDstPath) throws IllegalArgumentException, IOException{
		 Path dst =new Path(fs.getHomeDirectory(), fileDstPath);
		 fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		 return dst;
		
	}
	
	
	/**
	 * ��������Դ�ϴ���hdfs�������hdfs
	 * @param fs
	 * @param fileSrcPath
	 * @param fileDstPath
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public  Path getHdfsPath(String fileSrcPath) throws IllegalArgumentException, IOException{
		 Path dst =new Path(fs.getHomeDirectory(), fileSrcPath);
		 return dst;
		
	}
	
	
	/**
	 * ��hdfs�ϴ�����Դ�ļ���
	 * @param fs
	 * @param fileSrcPath
	 * @param fileDstPath
	 * @throws IOException 
	 */
	public Path mkFile(FileSystem fs,String resources,String fileDstPath) throws IOException{
		 Path dst =new Path(fs.getHomeDirectory(), fileDstPath);
		 FSDataOutputStream ostream = null;
	      try {
	        ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
	        ostream.writeUTF(resources);
	      } finally {
	        IOUtils.closeQuietly(ostream);
	      }
	      
	      return dst;
		
	}
	
	
	/**
	 * ����yarn����洢�ļ���ͳһ����Ŀ¼
	 * @param appName
	 * @param appId
	 * @param filename
	 * @return
	 */
	public String getPath(String appName,String appId,String filename){
		  if(filename.indexOf("/")>=0){
			  filename = filename.substring(filename.lastIndexOf("/")+1);
		  }
		  
		  String applicationMasterHdfsPath =appName + "/" + appId + "/" + filename;
		 return applicationMasterHdfsPath;
	}
	
	
	/**
	 * ����yarn����洢�ļ���ͳһ����Ŀ¼
	 * @param appName
	 * @param appId
	 * @param filename
	 * @return
	 */
	public String getlocalFilePathinHDFS(String appName,String appId,String filename){
		  if(filename.indexOf("/")>=0){
			  filename = filename.substring(filename.lastIndexOf("/")+1);
		  }
		  
		  String applicationMasterHdfsPath =appName + "/" + appId + "/local/"+filename;
		 return applicationMasterHdfsPath;
	}
	
	
	
	
	
	public FileStatus getFileStatus(Path fieldPath) throws IOException{
		return fs.getFileStatus(fieldPath);
	}
	
	

}
