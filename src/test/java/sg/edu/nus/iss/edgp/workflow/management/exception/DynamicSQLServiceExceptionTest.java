package sg.edu.nus.iss.edgp.workflow.management.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DynamicSQLServiceExceptionTest {

	@Test
	void constructor_withMessage_shouldSetMessage() {
		// Arrange
		String message = "SQL service failure";

		// Act
		DynamicSQLServiceException ex = new DynamicSQLServiceException(message);

		// Assert
		assertEquals(message, ex.getMessage());
		assertNull(ex.getCause(), "Cause should be null when only message is provided");
	}

	@Test
	void constructor_withMessageAndCause_shouldSetMessageAndCause() {
		// Arrange
		String message = "SQL execution error";
		Throwable cause = new RuntimeException("Underlying SQL exception");

		// Act
		DynamicSQLServiceException ex = new DynamicSQLServiceException(message, cause);

		// Assert
		assertEquals(message, ex.getMessage());
		assertSame(cause, ex.getCause(), "Cause should match the one provided to constructor");
	}
}
