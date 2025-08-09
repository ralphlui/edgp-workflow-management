package sg.edu.nus.iss.edgp.workflow.management.exception;

public class DynamicSQLServiceException extends RuntimeException  {

	private static final long serialVersionUID = 1L;

	public DynamicSQLServiceException(String message) {
		super(message);
	}

	public DynamicSQLServiceException(String message, Throwable cause) {
		super(message, cause);
	}
}
