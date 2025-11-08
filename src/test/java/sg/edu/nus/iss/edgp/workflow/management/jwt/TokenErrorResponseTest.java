package sg.edu.nus.iss.edgp.workflow.management.jwt;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TokenErrorResponseTest {

	@Test
	void sendErrorResponse_shouldSetCorrectJsonResponse() throws Exception {
		// Arrange
		MockHttpServletResponse response = new MockHttpServletResponse();
		String message = "Invalid JWT token";
		int status = HttpServletResponse.SC_UNAUTHORIZED;

		TokenErrorResponse.sendErrorResponse(response, message, status, "");

		// Assert
		assertEquals(status, response.getStatus());
		assertEquals("application/json", response.getContentType());

		String json = response.getContentAsString();
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> result = mapper.readValue(json, Map.class);

		assertEquals(false, result.get("success"));
		assertEquals(message, result.get("message"));
		assertEquals(0, result.get("totalRecord"));
		assertNull(result.get("data"));
		assertEquals(status, result.get("status"));
	}
}
