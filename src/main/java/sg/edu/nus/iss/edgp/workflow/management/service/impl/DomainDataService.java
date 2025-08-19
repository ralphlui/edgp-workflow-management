package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.exception.DomainDataServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DynamicSQLRepository;

@Service
@RequiredArgsConstructor
public class DomainDataService {

	private static final Logger logger = LoggerFactory.getLogger(DomainDataService.class);
	private final DynamicSQLRepository dynamicSQLRepository;

	public Map<Long, List<Map<String, Object>>> retrieveDomainDataList(String domainName, String orgId) {
		try {

			List<Map<String, Object>> domainDataList = dynamicSQLRepository.findAllDataList(domainName);

			long totalRecord = domainDataList.size();
			Map<Long, List<Map<String, Object>>> result = new HashMap<>();
			result.put(totalRecord, domainDataList);
			return result;

		} catch (Exception ex) {
			logger.error("Exception occurred while retrieving all data list", ex);
			throw new DomainDataServiceException("An error occurred while retrieving all data list", ex);

		}
	}

}
