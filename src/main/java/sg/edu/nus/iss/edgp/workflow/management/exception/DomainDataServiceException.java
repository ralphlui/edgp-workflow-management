package sg.edu.nus.iss.edgp.workflow.management.exception;

public class DomainDataServiceException extends RuntimeException  {

	private static final long serialVersionUID = 1L;

	public DomainDataServiceException(String message) {
		super(message);
	}

	public DomainDataServiceException(String message, Throwable cause) {
		super(message, cause);
	}

}
