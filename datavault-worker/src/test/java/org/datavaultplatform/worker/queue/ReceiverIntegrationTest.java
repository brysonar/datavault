package org.datavaultplatform.worker.queue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Ignore
@RunWith(SpringJUnit4ClassRunner.class)
// @ContextConfiguration(classes = {AppConfig.class})
@ContextConfiguration(locations = { "classpath:datavault-worker.xml" })
public class ReceiverIntegrationTest {

	private static final Logger logger = LoggerFactory.getLogger(ReceiverIntegrationTest.class);

	private static final boolean isRedeliver = false;
	private static final String bagId = "f8e65b18-756a-4b48-b26f-7b4eafa5c714";
	private static final String LOCAL_FILE_STORE = "c:/datavaultdump";
	private static final String ARCHIVE_FILE_STORE = "c:/tmp/datavault/archive";
	private static final String RETRIEVE_DUMP = "retrievedump/";

	private static final String depositMessage = "{\"taskClass\":\"org.datavaultplatform.worker.tasks.Deposit\",\"jobID\":\"218e741c-20fc-40a6-b861-7cfc9cde719d\",\"properties\":{\"bagId\":\""
			+ bagId
			+ "\",\"vaultMetadata\":\"{\\\"id\\\":\\\"762a2628-6145-4b0c-8208-4146503f4f57\\\",\\\"version\\\":1,\\\"creationTime\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"name\\\":\\\"frere\\\",\\\"description\\\":\\\"rere\\\",\\\"retentionPolicy\\\":{\\\"id\\\":\\\"BBSRC\\\",\\\"name\\\":\\\"BBSRC\\\",\\\"description\\\":\\\"Vaults are due for review 10 years after the date the last deposit was made.\\\",\\\"engine\\\":\\\"org.datavaultplatform.common.retentionpolicy.impl.uk.BBSRCRetentionPolicy\\\",\\\"sort\\\":3},\\\"retentionPolicyStatus\\\":1,\\\"retentionPolicyExpiry\\\":\\\"2027-07-03T12:30:09.000Z\\\",\\\"retentionPolicyLastChecked\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"user\\\":{\\\"id\\\":\\\"user1\\\",\\\"firstname\\\":\\\"user 1\\\",\\\"lastname\\\":\\\"Test\\\",\\\"password\\\":\\\"password1\\\",\\\"email\\\":null,\\\"admin\\\":false,\\\"properties\\\":null},\\\"dataset\\\":{\\\"id\\\":\\\"MOCK-DATASET-2\\\",\\\"name\\\":\\\"Sample dataset 2\\\"},\\\"size\\\":0}\",\"depositId\":\"4d069df9-e017-4333-8c53-c1dcf351ee21\",\"filePath\":\"myfiles/\",\"depositMetadata\":\"{\\\"id\\\":\\\"4d069df9-e017-4333-8c53-c1dcf351ee21\\\",\\\"version\\\":0,\\\"creationTime\\\":\\\"2017-07-03T12:30:57.662Z\\\",\\\"vault\\\":{\\\"id\\\":\\\"762a2628-6145-4b0c-8208-4146503f4f57\\\",\\\"version\\\":1,\\\"creationTime\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"name\\\":\\\"frere\\\",\\\"description\\\":\\\"rere\\\",\\\"retentionPolicy\\\":{\\\"id\\\":\\\"BBSRC\\\",\\\"name\\\":\\\"BBSRC\\\",\\\"description\\\":\\\"Vaults are due for review 10 years after the date the last deposit was made.\\\",\\\"engine\\\":\\\"org.datavaultplatform.common.retentionpolicy.impl.uk.BBSRCRetentionPolicy\\\",\\\"sort\\\":3},\\\"retentionPolicyStatus\\\":1,\\\"retentionPolicyExpiry\\\":\\\"2027-07-03T12:30:09.000Z\\\",\\\"retentionPolicyLastChecked\\\":\\\"2017-07-03T12:30:09.000Z\\\",\\\"user\\\":{\\\"id\\\":\\\"user1\\\",\\\"firstname\\\":\\\"user 1\\\",\\\"lastname\\\":\\\"Test\\\",\\\"password\\\":\\\"password1\\\",\\\"email\\\":null,\\\"admin\\\":false,\\\"properties\\\":null},\\\"dataset\\\":{\\\"id\\\":\\\"MOCK-DATASET-2\\\",\\\"name\\\":\\\"Sample dataset 2\\\"},\\\"size\\\":0},\\\"status\\\":\\\"NOT_STARTED\\\",\\\"note\\\":\\\"rrr\\\",\\\"bagId\\\":\\\""
			+ bagId
			+ "\\\",\\\"archiveDevice\\\":\\\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\\\",\\\"archiveId\\\":null,\\\"archiveSize\\\":0,\\\"archiveDigest\\\":null,\\\"archiveDigestAlgorithm\\\":null,\\\"fileOrigin\\\":\\\"Filesystem (local)\\\",\\\"shortFilePath\\\":\\\"myfiles/\\\",\\\"filePath\\\":\\\"c2acf748-8f77-4c2e-814b-be8eb53a9e26/myfiles/\\\",\\\"size\\\":0}\",\"externalMetadata\":\"Mock Metadata\",\"userId\":\"user1\"},\"userFileStore\":{\"id\":\"c2acf748-8f77-4c2e-814b-be8eb53a9e26\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Filesystem (local)\",\"properties\":{\"rootPath\":\""
			+ LOCAL_FILE_STORE
			+ "\"}},\"archiveFileStore\":{\"id\":\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Default archive store (local)\",\"properties\":{\"rootPath\":\""
			+ ARCHIVE_FILE_STORE + "\"}},\"redeliver\":false}";

	@Value("${tempDir}")
	private String tempDir;

	@Value("${metaDir}")
	private String metaDir;

	private String getReceiveMessage(String checkSum) {

		return " {\"taskClass\":\"org.datavaultplatform.worker.tasks.Retrieve\",\"jobID\":\"243fb683-fbf5-4362-a489-9eb4591aad88\",\"properties\":{\"archiveDigestAlgorithm\":\"SHA-1\",\"retrieveId\":\"694b8b57-6724-4c46-9a08-7492c37690d3\",\"bagId\":\""
				+ bagId + "\",\"depositId\":\"4d069df9-e017-4333-8c53-c1dcf351ee21\",\"retrievePath\":\""
				+ RETRIEVE_DUMP + "\",\"archiveSize\":\"20480\",\"userId\":\"user1\",\"archiveDigest\":\"" + checkSum
				+ "\",\"archiveId\":\"" + bagId
				+ ".tar\"},\"userFileStore\":{\"id\":\"c2acf748-8f77-4c2e-814b-be8eb53a9e26\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Filesystem (local)\",\"properties\":{\"rootPath\":\""
				+ LOCAL_FILE_STORE
				+ "\"}},\"archiveFileStore\":{\"id\":\"c0e19a74-a6e6-4b51-b070-10927e5c84b4\",\"storageClass\":\"org.datavaultplatform.common.storage.impl.LocalFileSystem\",\"label\":\"Default archive store (local)\",\"properties\":{\"rootPath\":\"c:/tmp/datavault/archive\"}},\"redeliver\":false}\r\n"
				+ "";
	}

	@Autowired
	private Receiver receiver;

	@Test
	public void testDeposit() throws IOException {

		delete(getArchivedTarFile());
		delete(getMetaDataDirectory());

		receiver.process(depositMessage, isRedeliver);

		Assert.assertTrue(getArchivedTarFile().toFile().exists());
		Assert.assertTrue(getMetaDataDirectory().toFile().exists());

		delete(getArchivedTarFile());
		delete(getMetaDataDirectory());
	}

	@Test
	public void testReceive() throws IOException {

		// set up data
		delete(getArchivedTarFile());
		delete(getMetaDataDirectory());
		delete(getRetrievedDirectory());

		receiver.process(depositMessage, isRedeliver);

		// run test
		String tagManifestFile = metaDir + "/" + bagId + "/tagmanifest.txt";
		String tarFileName = bagId + ".tar";

		Path path = Paths.get(tagManifestFile);

		String tarFileCheckSum = null;

		try (Stream<String> stream = Files.lines(path)) {

			String[] row = stream.filter(line -> line.contains(tarFileName)).map(line -> line.split(",")).findFirst()
					.get();

			tarFileCheckSum = row[0];
		}

		String receiveMessage = getReceiveMessage(tarFileCheckSum);
		receiver.process(receiveMessage, isRedeliver);

		delete(getArchivedTarFile());
		delete(getMetaDataDirectory());

	}

	private Path getRetrievedDirectory() {
		Path path = Paths.get(LOCAL_FILE_STORE + "/" + RETRIEVE_DUMP + bagId);
		return path;
	}

	private Path getArchivedTarFile() {
		Path path = Paths.get(ARCHIVE_FILE_STORE + "/" + bagId + ".tar");
		return path;
	}

	private Path getMetaDataDirectory() {
		Path path = Paths.get(metaDir + "/" + bagId);
		return path;
	}

	private void delete(Path path) throws IOException {

		if (Files.isDirectory(path)) {
			try (Stream<Path> stream = Files.list(path)) {
				stream.forEach(f -> {

					try {
						delete(f);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}

				});
			}
			// for (File sub : path.toFile().listFiles()) {
			// delete(sub.toPath());
			// }
		}

		// System.err.println("Deleting " + file.getAbsolutePath() + ",
		// directory: " + file.isDirectory());
		Files.deleteIfExists(path);
	}

}
