package sg.edu.nus.iss.edgp.workflow.management.exception;

public class WorkflowServiceException extends RuntimeException  {

	private static final long serialVersionUID = 1L;

	public WorkflowServiceException(String message) {
		super(message);
	}

	public WorkflowServiceException(String message, Throwable cause) {
		super(message, cause);
	}

}
