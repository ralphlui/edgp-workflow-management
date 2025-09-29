package sg.edu.nus.iss.edgp.workflow.management.service;

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Pageable;

public interface IDomainDataService {

	Map<Long, List<Map<String, Object>>> retrieveAllDomainDataList(String domainName, String userOrgId,
			String fileId,  Boolean includeArchive);
	
	Map<Long, List<Map<String, Object>>> retrievePaginatedDomainDataList(String domainName, String userOrgId,
			String fileId, Pageable pageable,  Boolean includeArchive);
	
	Map<String, Object> retrieveDetailDomainDataRecordById(String domainName, String id);
}
