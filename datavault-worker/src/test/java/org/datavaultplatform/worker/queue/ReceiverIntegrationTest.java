package org.datavaultplatform.worker.queue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(classes = {AppConfig.class})
@ContextConfiguration(locations = {
        "classpath:datavault-worker.xml"})
public class ReceiverIntegrationTest {


	//actual
//	173911D18B0BDF11132FA30FAB18A88DFD588803
//
//	!= 
//
	//expected
//	DB99C3AF5FC533D1909264520E999693DAADD698
	
	
	private static final boolean isRedeliver = false;

	private static final String depositMessage = "{\"taskClass\":\"org.datavaultplatform.worker.tasks.Deposit\",\"jobID\":\"218e741c-20fc-40a6-b861-7cfc9cde719d\",\"properties\":{\"bagId\":\"f8e65b18-756a-4b48-b26f-7b4eafa5c714\",\"vaultMetadata\":\"{\\\"id\\\":\\\"762a2628-6145-4b0c-8208-4146503f4f57\\\",\\\"version\\\":1,\\\"creationTime\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"name\\\":\\\"frere\\\",\\\"description\\\":\\\"rere\\\",\\\"retentionPolicy\\\":{\\\"id\\\":\\\"BBSRC\\\",\\\"name\\\":\\\"BBSRC\\\",\\\"description\\\":\\\"Vaults are due for review 10 years after the date the last deposit was made.\\\",\\\"engine\\\":\\\"org.datavaultplatform.common.retentionpolicy.impl.uk.BBSRCRetentionPolicy\\\",\\\"sort\\\":3},\\\"retentionPolicyStatus\\\":1,\\\"retentionPolicyExpiry\\\":\\\"2027-07-03T12:30:09.000Z\\\",\\\"retentionPolicyLastChecked\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"user\\\":{\\\"id\\\":\\\"user1\\\",\\\"firstname\\\":\\\"user 1\\\",\\\"lastname\\\":\\\"Test\\\",\\\"password\\\":\\\"password1\\\",\\\"email\\\":null,\\\"admin\\\":false,\\\"properties\\\":null},\\\"dataset\\\":{\\\"id\\\":\\\"MOCK-DATASET-2\\\",\\\"name\\\":\\\"Sample dataset 2\\\"},\\\"size\\\":0}\",\"depositId\":\"4d069df9-e017-4333-8c53-c1dcf351ee21\",\"filePath\":\"myfiles/\",\"depositMetadata\":\"{\\\"id\\\":\\\"4d069df9-e017-4333-8c53-c1dcf351ee21\\\",\\\"version\\\":0,\\\"creationTime\\\":\\\"2017-07-03T12:30:57.662Z\\\",\\\"vault\\\":{\\\"id\\\":\\\"762a2628-6145-4b0c-8208-4146503f4f57\\\",\\\"version\\\":1,\\\"creationTime\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"name\\\":\\\"frere\\\",\\\"description\\\":\\\"rere\\\",\\\"retentionPolicy\\\":{\\\"id\\\":\\\"BBSRC\\\",\\\"name\\\":\\\"BBSRC\\\",\\\"description\\\":\\\"Vaults are due for review 10 years after the date the last deposit was made.\\\",\\\"engine\\\":\\\"org.datavaultplatform.common.retentionpolicy.impl.uk.BBSRCRetentionPolicy\\\",\\\"sort\\\":3},\\\"retentionPolicyStatus\\\":1,\\\"retentionPolicyExpiry\\\":\\\"2027-07-03T12:30:09.000Z\\\",\\\"retentionPolicyLastChecked\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"user\\\":{\\\"id\\\":\\\"user1\\\",\\\"firstname\\\":\\\"user 1\\\",\\\"lastname\\\":\\\"Test\\\",\\\"password\\\":\\\"password1\\\",\\\"email\\\":null,\\\"admin\\\":false,\\\"properties\\\":null},\\\"dataset\\\":{\\\"id\\\":\\\"MOCK-DATASET-2\\\",\\\"name\\\":\\\"Sample dataset 2\\\"},\\\"size\\\":0},\\\"status\\\":\\\"NOT_STARTED\\\",\\\"note\\\":\\\"rrr\\\",\\\"bagId\\\":\\\"f8e65b18-756a-4b48-b26f-7b4eafa5c714\\\",\\\"archiveDevice\\\":\\\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\\\",\\\"archiveId\\\":null,\\\"archiveSize\\\":0,\\\"archiveDigest\\\":null,\\\"archiveDigestAlgorithm\\\":null,\\\"fileOrigin\\\":\\\"Filesystem (local)\\\",\\\"shortFilePath\\\":\\\"myfiles/\\\",\\\"filePath\\\":\\\"c2acf748-8f77-4c2e-814b-be8eb53a9e26/myfiles/\\\",\\\"size\\\":0}\",\"externalMetadata\":\"Mock Metadata\",\"userId\":\"user1\"},\"userFileStore\":{\"id\":\"c2acf748-8f77-4c2e-814b-be8eb53a9e26\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Filesystem (local)\",\"properties\":{\"rootPath\":\"c:/datavaultdump\"}},\"archiveFileStore\":{\"id\":\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Default archive store (local)\",\"properties\":{\"rootPath\":\"c:/tmp/datavault/archive\"}},\"redeliver\":false}";
	
	
//	private static final String receiveMessage = " {\"taskClass\":\"org.datavaultplatform.worker.tasks.Retrieve\",\"jobID\":\"243fb683-fbf5-4362-a489-9eb4591aad88\",\"properties\":{\"archiveDigestAlgorithm\":\"SHA-1\",\"retrieveId\":\"694b8b57-6724-4c46-9a08-7492c37690d3\",\"bagId\":\"f8e65b18-756a-4b48-b26f-7b4eafa5c714\",\"depositId\":\"4d069df9-e017-4333-8c53-c1dcf351ee21\",\"retrievePath\":\"retrievedump/\",\"archiveSize\":\"20480\",\"userId\":\"user1\",\"archiveDigest\":\"DB99C3AF5FC533D1909264520E999693DAADD698\",\"archiveId\":\"f8e65b18-756a-4b48-b26f-7b4eafa5c714.tar\"},\"userFileStore\":{\"id\":\"c2acf748-8f77-4c2e-814b-be8eb53a9e26\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Filesystem (local)\",\"properties\":{\"rootPath\":\"c:/datavaultdump\"}},\"archiveFileStore\":{\"id\":\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Default archive store (local)\",\"properties\":{\"rootPath\":\"c:/tmp/datavault/archive\"}},\"redeliver\":false}\r\n" + 
//			"";
	
	//with manually adjusted checksum for tar file
	private static final String receiveMessage = " {\"taskClass\":\"org.datavaultplatform.worker.tasks.Retrieve\",\"jobID\":\"243fb683-fbf5-4362-a489-9eb4591aad88\",\"properties\":{\"archiveDigestAlgorithm\":\"SHA-1\",\"retrieveId\":\"694b8b57-6724-4c46-9a08-7492c37690d3\",\"bagId\":\"f8e65b18-756a-4b48-b26f-7b4eafa5c714\",\"depositId\":\"4d069df9-e017-4333-8c53-c1dcf351ee21\",\"retrievePath\":\"retrievedump/\",\"archiveSize\":\"20480\",\"userId\":\"user1\",\"archiveDigest\":\"173911D18B0BDF11132FA30FAB18A88DFD588803\",\"archiveId\":\"f8e65b18-756a-4b48-b26f-7b4eafa5c714.tar\"},\"userFileStore\":{\"id\":\"c2acf748-8f77-4c2e-814b-be8eb53a9e26\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Filesystem (local)\",\"properties\":{\"rootPath\":\"c:/datavaultdump\"}},\"archiveFileStore\":{\"id\":\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Default archive store (local)\",\"properties\":{\"rootPath\":\"c:/tmp/datavault/archive\"}},\"redeliver\":false}\r\n" + 
			"";
	
	@Autowired
	private Receiver receiver;


	@Test
	public void testDeposit() {

		receiver.process(depositMessage, isRedeliver);
	}
	
	@Test
	public void testReceive() {

		receiver.process(receiveMessage, isRedeliver);
	}
}
