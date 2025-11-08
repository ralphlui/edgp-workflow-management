package sg.edu.nus.iss.edgp.workflow.management.exception;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WorkflowServiceExceptionTest {

	@Test
	void constructor_withMessage_shouldSetMessage() {
		// Arrange
		String errorMessage = "Something went wrong";

		// Act
		WorkflowServiceException ex = new WorkflowServiceException(errorMessage);

		// Assert
		assertEquals(errorMessage, ex.getMessage());
		assertNull(ex.getCause(), "Cause should be null when not provided");
	}

	@Test
	void constructor_withMessageAndCause_shouldSetMessageAndCause() {
		// Arrange
		String errorMessage = "Another error";
		Throwable cause = new RuntimeException("Root cause");

		// Act
		WorkflowServiceException ex = new WorkflowServiceException(errorMessage, cause);

		// Assert
		assertEquals(errorMessage, ex.getMessage());
		assertSame(cause, ex.getCause(), "Cause should match the one passed to constructor");
	}
}