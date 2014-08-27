package com.hope.hopeOnYarn.model;

public class AppInfo {
	
	private String applocation;
	
	private String apptimestamp;
	
	private String applen;
	
	private String appName;
	
	private String mainClass;
	
	private String rootDir;
	
	private String appHDFSFiles;
	
	private String appLocalFiles;
	
	

	public String getRootDir() {
		return rootDir;
	}

	public void setRootDir(String rootDir) {
		this.rootDir = rootDir;
	}


	public String getApplocation() {
		return applocation;
	}

	public void setApplocation(String applocation) {
		this.applocation = applocation;
	}

	public String getApptimestamp() {
		return apptimestamp;
	}

	public void setApptimestamp(String apptimestamp) {
		this.apptimestamp = apptimestamp;
	}

	public String getApplen() {
		return applen;
	}

	public void setApplen(String applen) {
		this.applen = applen;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getMainClass() {
		return mainClass;
	}

	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}

	public String getAppHDFSFiles() {
		return appHDFSFiles;
	}

	public void setAppHDFSFiles(String appHDFSFiles) {
		this.appHDFSFiles = appHDFSFiles;
	}

	public String getAppLocalFiles() {
		return appLocalFiles;
	}

	public void setAppLocalFiles(String appLocalFiles) {
		this.appLocalFiles = appLocalFiles;
	}
	
	
	

}
