package sg.edu.nus.iss.edgp.workflow.management.exception;

public class DynamicDynamoServiceException extends RuntimeException  {

	private static final long serialVersionUID = 1L;

	public DynamicDynamoServiceException(String message) {
		super(message);
	}

	public DynamicDynamoServiceException(String message, Throwable cause) {
		super(message, cause);
	}


}
