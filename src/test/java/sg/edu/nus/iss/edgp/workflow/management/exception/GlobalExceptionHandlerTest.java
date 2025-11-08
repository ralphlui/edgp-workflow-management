package sg.edu.nus.iss.edgp.workflow.management.exception;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;

import sg.edu.nus.iss.edgp.workflow.management.dto.APIResponse;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

@SpringBootTest
@ActiveProfiles("test")
public class GlobalExceptionHandlerTest {

	@Autowired
	private GlobalExceptionHandler globalExceptionHandler;

	@MockitoBean
	private JwtDecoder jwtDecoder;

	@SuppressWarnings("rawtypes")
	@Test
	void testHandleObjectNotFoundException() {
		Exception ex = new Exception("Test exception message");

		ResponseEntity<APIResponse> responseEntity = globalExceptionHandler.handleObjectNotFoundException(ex);

		// Verify the result
		assertEquals(HttpStatus.UNAUTHORIZED, responseEntity.getStatusCode());
		assertEquals("Failed to get data. Test exception message", responseEntity.getBody().getMessage());
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testIllegalArgumentException() {
		IllegalArgumentException ex = new IllegalArgumentException("Test illegal argument message");

		ResponseEntity<APIResponse> responseEntity = globalExceptionHandler.illegalArgumentException(ex);

		assertEquals(HttpStatus.BAD_REQUEST, responseEntity.getStatusCode());
		assertEquals("Invalid data: Test illegal argument message", responseEntity.getBody().getMessage());
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testHandleValidationException() throws NoSuchMethodException {
		// Create mock BindingResult
		BindingResult bindingResult = mock(BindingResult.class);

		// Add field errors
		FieldError fieldError1 = new FieldError("objectName", "page", "must be a positive integer");
		FieldError fieldError2 = new FieldError("objectName", "username", "must not be blank");

		when(bindingResult.getFieldErrors()).thenReturn(Arrays.asList(fieldError1, fieldError2));

		// Create mock MethodArgumentNotValidException
		MethodArgumentNotValidException exception = new MethodArgumentNotValidException(null, bindingResult);

		// Call handler
		ResponseEntity<APIResponse> responseEntity = globalExceptionHandler.handleValidationException(exception);

		// Validate response
		assertEquals(HttpStatus.BAD_REQUEST, responseEntity.getStatusCode());
		String message = responseEntity.getBody().getMessage();

		assertTrue(message.contains("The 'page' field must be a valid positive integer."));
		assertTrue(message.contains("Invalid value for 'username': must not be blank."));
	}
}
