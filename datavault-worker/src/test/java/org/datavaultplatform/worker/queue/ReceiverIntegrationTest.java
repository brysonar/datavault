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

	
	private static final boolean isRedeliver = false;
	private static final String message = "{\"taskClass\":\"org.datavaultplatform.worker.tasks.Deposit\",\"jobID\":\"ed4bb8ff-1a37-4430-b3fd-8f1b4e2d0a38\",\"properties\":{\"bagId\":\"6ebd4a0c-cda6-4adf-a18f-e6c9a58f6351\",\"vaultMetadata\":\"{\\\"id\\\":\\\"a0e93011-0043-45fa-8a14-f768917858e9\\\",\\\"version\\\":1,\\\"creationTime\\\":\\\"2017-06-26T08:11:57.000Z\\\",\\\"name\\\":\\\"vault name\\\",\\\"description\\\":\\\"desc\\\",\\\"retentionPolicy\\\":{\\\"id\\\":\\\"UNIVERSITY\\\",\\\"name\\\":\\\"Default University Policy\\\",\\\"description\\\":\\\"Default University policy that flags vaults for review after 5 years.\\\",\\\"engine\\\":\\\"org.datavaultplatform.common.retentionpolicy.impl.DefaultRetentionPolicy\\\",\\\"sort\\\":1},\\\"retentionPolicyStatus\\\":1,\\\"retentionPolicyExpiry\\\":\\\"2022-06-26T08:11:57.000Z\\\",\\\"retentionPolicyLastChecked\\\":\\\"2017-06-26T08:11:57.000Z\\\",\\\"user\\\":{\\\"id\\\":\\\"user1\\\",\\\"firstname\\\":\\\"user 1\\\",\\\"lastname\\\":\\\"Test\\\",\\\"password\\\":\\\"password1\\\",\\\"email\\\":null,\\\"admin\\\":false,\\\"properties\\\":null},\\\"dataset\\\":{\\\"id\\\":\\\"MOCK-DATASET-1\\\",\\\"name\\\":\\\"Sample dataset 1\\\"},\\\"size\\\":0}\",\"depositId\":\"eda391c0-deb8-4af5-b0d9-01781439fca4\",\"filePath\":\"myfiles/\",\"depositMetadata\":\"{\\\"id\\\":\\\"eda391c0-deb8-4af5-b0d9-01781439fca4\\\",\\\"version\\\":0,\\\"creationTime\\\":\\\"2017-06-26T08:12:13.775Z\\\",\\\"vault\\\":{\\\"id\\\":\\\"a0e93011-0043-45fa-8a14-f768917858e9\\\",\\\"version\\\":1,\\\"creationTime\\\":\\\"2017-06-26T08:11:57.000Z\\\",\\\"name\\\":\\\"vault name\\\",\\\"description\\\":\\\"desc\\\",\\\"retentionPolicy\\\":{\\\"id\\\":\\\"UNIVERSITY\\\",\\\"name\\\":\\\"Default University Policy\\\",\\\"description\\\":\\\"Default University policy that flags vaults for review after 5 years.\\\",\\\"engine\\\":\\\"org.datavaultplatform.common.retentionpolicy.impl.DefaultRetentionPolicy\\\",\\\"sort\\\":1},\\\"retentionPolicyStatus\\\":1,\\\"retentionPolicyExpiry\\\":\\\"2022-06-26T08:11:57.000Z\\\",\\\"retentionPolicyLastChecked\\\":\\\"2017-06-26T08:11:57.000Z\\\",\\\"user\\\":{\\\"id\\\":\\\"user1\\\",\\\"firstname\\\":\\\"user 1\\\",\\\"lastname\\\":\\\"Test\\\",\\\"password\\\":\\\"password1\\\",\\\"email\\\":null,\\\"admin\\\":false,\\\"properties\\\":null},\\\"dataset\\\":{\\\"id\\\":\\\"MOCK-DATASET-1\\\",\\\"name\\\":\\\"Sample dataset 1\\\"},\\\"size\\\":0},\\\"status\\\":\\\"NOT_STARTED\\\",\\\"note\\\":\\\"deposit notes\\\",\\\"bagId\\\":\\\"6ebd4a0c-cda6-4adf-a18f-e6c9a58f6351\\\",\\\"archiveDevice\\\":\\\"6206550f-246a-4eff-83ac-4045262e209c\\\",\\\"archiveId\\\":null,\\\"archiveSize\\\":0,\\\"archiveDigest\\\":null,\\\"archiveDigestAlgorithm\\\":null,\\\"fileOrigin\\\":\\\"Filesystem (local)\\\",\\\"shortFilePath\\\":\\\"myfiles/\\\",\\\"filePath\\\":\\\"e3835b7a-e43a-45d5-8024-604683efb3da/myfiles/\\\",\\\"size\\\":0}\",\"externalMetadata\":\"Mock Metadata\",\"userId\":\"user1\"},\"userFileStore\":{\"id\":\"e3835b7a-e43a-45d5-8024-604683efb3da\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Filesystem (local)\",\"properties\":{\"rootPath\":\"c:/datavaultdump\"}},\"archiveFileStore\":{\"id\":\"6206550f-246a-4eff-83ac-4045262e209c\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Default archive store (local)\",\"properties\":{\"rootPath\":\"c:/tmp/datavault/archive\"}},\"redeliver\":false}\r\n" + 
			"";
	
	@Autowired
	private Receiver receiver;


	@Test
	public void test() {

		receiver.process(message, isRedeliver);
	}
}
