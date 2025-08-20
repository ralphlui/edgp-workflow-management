package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.exception.DomainDataServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DomainDataRepository;
import sg.edu.nus.iss.edgp.workflow.management.service.IDomainDataService;

@Service
@RequiredArgsConstructor
public class DomainDataService implements IDomainDataService {

	private static final Logger logger = LoggerFactory.getLogger(DomainDataService.class);
	private final DomainDataRepository domainDataRepository;

	@Override
	public Map<Long, List<Map<String, Object>>> retrieveAllDomainDataList(String domainName, String userOrgId,
			String fileId) {
		try {

			List<Map<String, Object>> domainDataList = domainDataRepository.findAllDomainDataList(domainName, userOrgId,
					fileId);
			long totalRecord = domainDataList.size();
			Map<Long, List<Map<String, Object>>> result = new HashMap<>();
			result.put(totalRecord, domainDataList);
			logger.info("All domain data list count. {}", totalRecord);
			return result;

		} catch (Exception ex) {
			logger.error("Exception occurred while retrieving all data list", ex);
			throw new DomainDataServiceException("An error occurred while retrieving all data list", ex);

		}
	}

	@Override
	public Map<Long, List<Map<String, Object>>> retrievePaginatedDomainDataList(String domainName, String userOrgId,
			String fileId, Pageable pageable) {
		try {

			Page<Map<String, Object>> domainDataList = domainDataRepository.findPaginatedDomainDataList(domainName,
					userOrgId, fileId, pageable);
			long totalRecord = domainDataList.getTotalElements();
			Map<Long, List<Map<String, Object>>> result = new HashMap<>();
			result.put(totalRecord, domainDataList.getContent());
			logger.info("Paginated domain data list count. {}", totalRecord);
			return result;

		} catch (Exception ex) {
			logger.error("Exception occurred while retrieving all data list", ex);
			throw new DomainDataServiceException("An error occurred while retrieving all data list", ex);

		}
	}

	@Override
	public Map<String, Object> retrieveDetailDomainDataRecordById(String domainName, String id) {

		try {

			Map<String, Object> domainDataRecord = domainDataRepository.retrieveDetailDomainDataRecordById(domainName,
					id);

			if (domainDataRecord == null || domainDataRecord.isEmpty()) {
				throw new DomainDataServiceException(String.format("No detail domain record found for domain"));
			}

			logger.debug("Retrieved detail domain data record for domain='{}', id='{}'", domainName, id);
			return domainDataRecord;

		} catch (Exception ex) {
			logger.error("Unexpected error retrieving record for domain", ex);
			throw new DomainDataServiceException("Unexpected error retrieving domain data record by id", ex);
		}
	}

}
