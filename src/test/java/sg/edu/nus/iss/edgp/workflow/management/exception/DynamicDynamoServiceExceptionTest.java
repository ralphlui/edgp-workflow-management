package sg.edu.nus.iss.edgp.workflow.management.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DynamicDynamoServiceExceptionTest {

	@Test
	void constructor_withMessage_shouldSetMessage() {
		// Arrange
		String message = "Dynamo service failed";

		// Act
		DynamicDynamoServiceException ex = new DynamicDynamoServiceException(message);

		// Assert
		assertEquals(message, ex.getMessage());
		assertNull(ex.getCause(), "Cause should be null when not provided");
	}

	@Test
	void constructor_withMessageAndCause_shouldSetMessageAndCause() {
		// Arrange
		String message = "Dynamo service error";
		Throwable cause = new RuntimeException("Underlying DynamoDB failure");

		// Act
		DynamicDynamoServiceException ex = new DynamicDynamoServiceException(message, cause);

		// Assert
		assertEquals(message, ex.getMessage());
		assertSame(cause, ex.getCause(), "Cause should match the one provided");
	}
}