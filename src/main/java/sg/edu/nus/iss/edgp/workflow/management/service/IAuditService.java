package sg.edu.nus.iss.edgp.workflow.management.service;

import sg.edu.nus.iss.edgp.workflow.management.dto.AuditDTO;

public interface IAuditService {
	void sendMessage(AuditDTO autAuditDTO,String token);
 
}
