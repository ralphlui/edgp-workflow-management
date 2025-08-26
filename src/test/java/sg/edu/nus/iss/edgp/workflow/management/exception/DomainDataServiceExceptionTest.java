package sg.edu.nus.iss.edgp.workflow.management.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DomainDataServiceExceptionTest {

	@Test
	void constructor_withMessage_shouldSetMessage() {
		// Arrange
		String message = "Domain data service failed";

		// Act
		DomainDataServiceException ex = new DomainDataServiceException(message);

		// Assert
		assertEquals(message, ex.getMessage());
		assertNull(ex.getCause(), "Cause should be null when not provided");
	}

	@Test
	void constructor_withMessageAndCause_shouldSetMessageAndCause() {
		// Arrange
		String message = "Domain data error";
		Throwable cause = new RuntimeException("Underlying failure");

		// Act
		DomainDataServiceException ex = new DomainDataServiceException(message, cause);

		// Assert
		assertEquals(message, ex.getMessage());
		assertSame(cause, ex.getCause(), "Cause should match the one provided");
	}
}