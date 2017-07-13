package org.datavaultplatform.worker.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TarUtil {
    
	private static final Logger logger = LoggerFactory.getLogger(TarUtil.class);

    // Create a TAR archive of a directory.
	public static boolean createTar(File dir, File output) {

		logger.debug("Creating tar - source: {}, tarfile: {}", dir.getAbsolutePath(), output.getAbsolutePath());

		try (FileOutputStream fos = new FileOutputStream(output);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(bos)) {

			tarArchiveOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
			tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
			addFileToTar(tarArchiveOutputStream, dir, "");
			tarArchiveOutputStream.finish();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return true;
	}
    
    // Recursively add a file or directory to a TAR archive.
	private static void addFileToTar(TarArchiveOutputStream tarArchiveOutputStream, File file, String base) {

		try {
			String entryName = base + file.getName();
			TarArchiveEntry tarEntry = new TarArchiveEntry(file, entryName);
			tarArchiveOutputStream.putArchiveEntry(tarEntry);

			if (file.isFile()) {
				try (FileInputStream in = new FileInputStream(file)) {
					IOUtils.copy(in, tarArchiveOutputStream);
				}
				tarArchiveOutputStream.closeArchiveEntry();
			} else {
				tarArchiveOutputStream.closeArchiveEntry();
				File[] children = file.listFiles();
				if (children != null) {
					for (File child : children) {
						addFileToTar(tarArchiveOutputStream, child, entryName + "/");
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    // Extract the contents of a TAR archive to a directory.
	public static File unTar(File input, Path outputDir) {

		File topDir = null;

		try (
			FileInputStream fis = new FileInputStream(input);
			BufferedInputStream bis = new BufferedInputStream(fis);
			TarArchiveInputStream tar = new TarArchiveInputStream(bis)) {

			TarArchiveEntry entry;
			while ((entry = tar.getNextTarEntry()) != null) {

				Path path = outputDir.resolve(entry.getName());
				File entryFile = path.toFile();

				if (entry.isDirectory()) {
					// Create a directory
					entryFile.mkdir();

					if (topDir == null) {
						topDir = entryFile;
					}
				} else {
					// Extract a single file
					FileOutputStream fos = new FileOutputStream(entryFile);
					IOUtils.copyLarge(tar, fos, 0, entry.getSize());
					fos.close();
				}
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return topDir;
	}
}
