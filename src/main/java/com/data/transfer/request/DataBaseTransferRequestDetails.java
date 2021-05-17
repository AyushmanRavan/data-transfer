package com.data.transfer.request;

public class DataBaseTransferRequestDetails {

	private String sourceDriverClassName;
	private String sourceConnectionURL;
	private String sourceUserName;
	private String sourcePassword;
	private String sourceDataBase;

	private String destinationDriverClassName;
	private String destinationConnectionURL;
	private String destinationUserName;
	private String destinationPassword;
	private String destinationDataBase;

	private String sourceTableName, destinationTableName;

	public String getSourceDriverClassName() {
		return sourceDriverClassName;
	}

	public void setSourceDriverClassName(String sourceDriverClassName) {
		this.sourceDriverClassName = sourceDriverClassName;
	}

	public String getSourceConnectionURL() {
		return sourceConnectionURL;
	}

	public void setSourceConnectionURL(String sourceConnectionURL) {
		this.sourceConnectionURL = sourceConnectionURL;
	}

	public String getSourceUserName() {
		return sourceUserName;
	}

	public void setSourceUserName(String sourceUserName) {
		this.sourceUserName = sourceUserName;
	}

	public String getSourcePassword() {
		return sourcePassword;
	}

	public void setSourcePassword(String sourcePassword) {
		this.sourcePassword = sourcePassword;
	}

	public String getSourceDataBase() {
		return sourceDataBase;
	}

	public void setSourceDataBase(String sourceDataBase) {
		this.sourceDataBase = sourceDataBase;
	}

	public String getDestinationDriverClassName() {
		return destinationDriverClassName;
	}

	public void setDestinationDriverClassName(String destinationDriverClassName) {
		this.destinationDriverClassName = destinationDriverClassName;
	}

	public String getDestinationConnectionURL() {
		return destinationConnectionURL;
	}

	public void setDestinationConnectionURL(String destinationConnectionURL) {
		this.destinationConnectionURL = destinationConnectionURL;
	}

	public String getDestinationUserName() {
		return destinationUserName;
	}

	public void setDestinationUserName(String destinationUserName) {
		this.destinationUserName = destinationUserName;
	}

	public String getDestinationPassword() {
		return destinationPassword;
	}

	public void setDestinationPassword(String destinationPassword) {
		this.destinationPassword = destinationPassword;
	}

	public String getDestinationDataBase() {
		return destinationDataBase;
	}

	public void setDestinationDataBase(String destinationDataBase) {
		this.destinationDataBase = destinationDataBase;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public String getDestinationTableName() {
		return destinationTableName;
	}

	public void setDestinationTableName(String destinationTableName) {
		this.destinationTableName = destinationTableName;
	}

}
