package com.data.transfer.response;

public class ConnectionStatusResponse {

	private boolean status;
	private String message;

	public ConnectionStatusResponse() {
	}

	public ConnectionStatusResponse(boolean status, String message) {
		this.status = status;
		this.message = message;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "ConnectionStatusResponse [status=" + status + ", message=" + message + "]";
	}

}
