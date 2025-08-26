package sg.edu.nus.iss.edgp.workflow.management.jwt;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletResponse;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.AuditService;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;

import io.jsonwebtoken.JwtException;

import org.springframework.security.core.context.SecurityContextHolder;

public class JwtFilterTest {

	@InjectMocks
	private JWTFilter jwtFilter;

	@Mock
	private JWTService jwtService;

	@Mock
	private AuditService auditService;

	@Mock
	private FilterChain filterChain;

	@Mock
	private UserDetails userDetails;

	private MockHttpServletRequest request;
	private MockHttpServletResponse response;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
		jwtFilter = new JWTFilter(jwtService, auditService);
		request = new MockHttpServletRequest();
		response = new MockHttpServletResponse();
	}

	@Test
	void testDoFilterInternal_ValidToken() throws JwtException, IllegalArgumentException, Exception {
		String token = "valid.jwt.token";
		request.addHeader("Authorization", "Bearer " + token);
		request.setRequestURI("/test");
		request.setMethod("GET");

		when(jwtService.getUserDetail(anyString(), eq(token))).thenReturn(userDetails);
		when(jwtService.validateToken(eq(token), eq(userDetails))).thenReturn(true);
		when(userDetails.getAuthorities()).thenReturn(null);

		jwtFilter.doFilterInternal(request, response, filterChain);

		verify(filterChain, times(1)).doFilter(request, response);
		assertNotNull(SecurityContextHolder.getContext().getAuthentication());
		assertTrue(
				SecurityContextHolder.getContext().getAuthentication() instanceof UsernamePasswordAuthenticationToken);
	}

	@Test
	void testDoFilterInternal_MissingAuthorizationHeader() throws ServletException, IOException {
		request.setRequestURI("/test");
		request.setMethod("GET");

		jwtFilter.doFilterInternal(request, response, filterChain);

		assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
	}
}
