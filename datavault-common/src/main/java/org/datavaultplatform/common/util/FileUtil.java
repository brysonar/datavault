package org.datavaultplatform.common.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.datavaultplatform.common.exception.DataVaultException;
import org.datavaultplatform.common.io.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileUtil {

	private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
	private static final String CANNOT_PERFORM_COPY_PREFIX = "Failed to perform copy ";
	
	private FileUtil() {
		super();
	}

	/**
	 * Copy file or directory
	 * 
	 * @param source
	 * @param target
	 * @throws IOException 
	 */
	public static void copyDirectoryOrFile(final Progress progress, final File source, final File target) {

		copyDirectoryOrFile(progress, source.toPath(), target.toPath());
	}
	
	/**
	 * Copy file or directory
	 * 
	 * @param source
	 * @param target
	 * @throws IOException 
	 */
	public static void copyDirectoryOrFile(final Progress progress, final Path source, final Path target) {

		try {
	        if (source.toFile().isFile()) {
	        	logger.debug("Copying file: {}", source);
	        } else if (source.toFile().isDirectory()) {
	        	logger.debug("Copying directory: {}", source);
	        }
	        	
			// Files.walkFileTree(source, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, vistor);
			Files.walkFileTree(source, new CopyFileVisitor(source, target, progress));
		} catch (Exception e) {
			throw new DataVaultException(CANNOT_PERFORM_COPY_PREFIX + " from " + source + " to " + target + " - " + e.getMessage(), e);
		}
	}
	
	static class CopyFileVisitor extends SimpleFileVisitor<Path> {
		
		private final Path source;
		private final Path target;
		final Progress progress;
		
		public CopyFileVisitor(Path source, Path target, Progress progress) {
			this.target = target;
			this.source = source;
			this.progress = progress; 
		}
		
		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

			Path targetdir = target.resolve(source.relativize(dir));
			logger.trace("Pre visit directory - source: " + source, ", dir: " + dir + ", targetdir: " + targetdir);
			try {
				Files.copy(dir, targetdir);
			} catch (FileAlreadyExistsException e) {
				String type = Files.isDirectory(targetdir) ? "directory" : "file";			
				throw new DataVaultException(type + " already exists: " + e.getMessage(), e);
			}
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						
			if (!target.getParent().toFile().isDirectory()) {
				throw new DataVaultException("target directory " + target.getParent() + " does not exist");
			}
			
			try {
				Files.copy(file, target.resolve(source.relativize(file)));
			} catch (FileAlreadyExistsException e) {
				throw new DataVaultException("file already exists: " + e.getMessage(), e);
			}
			
			if (progress != null) {
				progress.timestamp = System.currentTimeMillis();
				progress.fileCount += 1;
			}
			
			return FileVisitResult.CONTINUE;
		}
	}

}
