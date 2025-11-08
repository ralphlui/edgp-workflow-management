package sg.edu.nus.iss.edgp.workflow.management.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import sg.edu.nus.iss.edgp.workflow.management.api.connector.OrganizationAPICall;
import sg.edu.nus.iss.edgp.workflow.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.workflow.management.strategy.impl.ValidationStrategy;
import sg.edu.nus.iss.edgp.workflow.management.utility.JSONReader;

@ExtendWith(MockitoExtension.class)
class ValidationStrategyTest {

	@Mock
	private JSONReader jsonReader;
	@Mock
	private OrganizationAPICall orgAPICall;

	private ValidationStrategy strategy;

	private static final String AUTH = "Bearer abc";
	private static final String USER_ORG = "org_123";
	private static final String OTHER_ORG = "org_999";

	@BeforeEach
	void setUp() {
		strategy = new ValidationStrategy(jsonReader, orgAPICall);
	}

	private void mockActiveCheck(boolean activeFlag) {
		// Return any valid JSON so JSONParser can parse it
		when(orgAPICall.validateActiveOrganization(eq(USER_ORG), eq(AUTH))).thenReturn("{}");
		when(jsonReader.getSuccessFromResponse(any(JSONObject.class))).thenReturn(true);
		JSONObject data = new JSONObject();
		data.put("active", activeFlag);
		when(jsonReader.getDataFromResponse(any(JSONObject.class))).thenReturn(data);
	}

	@Test
	void isUserOrganizationActive_active_returnsValid() {
		mockActiveCheck(true);

		ValidationResult result = strategy.isUserOrganizationActive(USER_ORG, AUTH);

		assertThat(result.isValid()).isTrue();
		assertThat(result.getMessage()).isNull();
		verify(orgAPICall).validateActiveOrganization(USER_ORG, AUTH);
	}

	@Test
	void isUserOrganizationActive_inactive_returnsInvalidWithMessage() {
		mockActiveCheck(false);

		ValidationResult result = strategy.isUserOrganizationActive(USER_ORG, AUTH);

		assertThat(result.isValid()).isFalse();
		assertThat(result.getMessage()).isEqualTo("Invalid organization. Unable to view data.");
		assertThat(result.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
	}

	@Test
	void isUserOrganizationActive_blankUserOrg_returnsInvalid() {
		ValidationResult r1 = strategy.isUserOrganizationActive(null, AUTH);
		ValidationResult r2 = strategy.isUserOrganizationActive("   ", AUTH);

		assertThat(r1.isValid()).isFalse();
		assertThat(r1.getMessage()).isEqualTo("Organization ID missing or invalid in token");
		assertThat(r1.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);

		assertThat(r2.isValid()).isFalse();
		assertThat(r2.getMessage()).isEqualTo("Organization ID missing or invalid in token");
		assertThat(r2.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);

		verifyNoInteractions(orgAPICall);
	}

	@Test
	void isUserOrganizationValidAndActive_activeAndIdsMatch_returnsValid() {
		mockActiveCheck(true);

		ValidationResult result = strategy.isUserOrganizationValidAndActive(USER_ORG, USER_ORG, AUTH);

		assertThat(result.isValid()).isTrue();
		verify(orgAPICall).validateActiveOrganization(USER_ORG, AUTH);
	}

	@Test
	void isUserOrganizationValidAndActive_activeButIdsMismatch_returnsUnauthorized() {
		mockActiveCheck(true);

		ValidationResult result = strategy.isUserOrganizationValidAndActive(OTHER_ORG, USER_ORG, AUTH);

		assertThat(result.isValid()).isFalse();
		assertThat(result.getMessage()).isEqualTo("Unauthorized to view this data.");
		assertThat(result.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
		verify(orgAPICall).validateActiveOrganization(USER_ORG, AUTH);
	}

	@Test
	void isUserOrganizationValidAndActive_inactiveShortCircuits_returnsInvalidActiveMessage() {
		mockActiveCheck(false);

		ValidationResult result = strategy.isUserOrganizationValidAndActive(OTHER_ORG, USER_ORG, AUTH);

		// Should fail on active check and not replace message with "Unauthorized..."
		assertThat(result.isValid()).isFalse();
		assertThat(result.getMessage()).isEqualTo("Invalid organization. Unable to view data.");
		assertThat(result.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
		verify(orgAPICall).validateActiveOrganization(USER_ORG, AUTH);
	}

	@Test
	void validateDomainAndOrgAccess_blankDomain_returnsInvalid() {
		ValidationResult result1 = strategy.validateDomainAndOrgAccess(null, USER_ORG, AUTH);
		ValidationResult result2 = strategy.validateDomainAndOrgAccess("   ", USER_ORG, AUTH);

		assertThat(result1.isValid()).isFalse();
		assertThat(result1.getMessage()).isEqualTo("Domain name is missing");
		assertThat(result1.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);

		assertThat(result2.isValid()).isFalse();
		assertThat(result2.getMessage()).isEqualTo("Domain name is missing");
		assertThat(result2.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);

		verifyNoInteractions(orgAPICall);
	}

	@Test
	void validateDomainAndOrgAccess_domainProvidedAndActive_returnsValid() {
		mockActiveCheck(true);

		ValidationResult result = strategy.validateDomainAndOrgAccess("example.com", USER_ORG, AUTH);

		assertThat(result.isValid()).isTrue();
		verify(orgAPICall).validateActiveOrganization(USER_ORG, AUTH);
	}

	@Test
	void validateActiveOrganization_malformedJson_gracefullyReturnsInvalid() {
		// Return malformed JSON to trigger ParseException inside
		// validateActiveOrganization
		when(orgAPICall.validateActiveOrganization(eq(USER_ORG), eq(AUTH))).thenReturn("this is not json");

		ValidationResult result = strategy.isUserOrganizationActive(USER_ORG, AUTH);

		assertThat(result.isValid()).isFalse();
		assertThat(result.getMessage()).isEqualTo("Invalid organization. Unable to view data.");
		assertThat(result.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
		// JSONReader shouldn't be consulted when parsing fails
		verifyNoInteractions(jsonReader);
	}
}