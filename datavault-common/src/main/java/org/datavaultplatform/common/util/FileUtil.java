package org.datavaultplatform.common.util;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.datavaultplatform.common.exception.DataVaultException;

public final class FileUtil {

	private static final String CANNOT_PERFORM_COPY_PREFIX = "Cannot perform copy - ";
	
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
	public static void copyFile(final Path source, final Path target) {

		try {
			// Files.walkFileTree(source, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, vistor);
			Files.walkFileTree(source, new CopyFileVisitor(source, target));
		} catch (Exception e) {
			throw new DataVaultException(CANNOT_PERFORM_COPY_PREFIX + e.getMessage(), e);
		}
	}
	
	static class CopyFileVisitor extends SimpleFileVisitor<Path> {
		
		private final Path source;
		private final Path target;
		
		public CopyFileVisitor(Path source, Path target) {
			this.target = target;
			this.source = source;
		}
		
		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

			Path targetdir = target.resolve(source.relativize(dir));
			
//			System.err.println("pre visit directory - source: " + source);
//			System.err.println("pre visit directory - dir: " + dir);
//			System.err.println("pre visit directory - targetdir: " + targetdir);
			
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
				throw new DataVaultException("Target directory " + target.getParent() + " does not exist");
			}
			
			try {
				Files.copy(file, target.resolve(source.relativize(file)));
			} catch (FileAlreadyExistsException e) {
				throw new DataVaultException("file already exists: " + e.getMessage(), e);
			}
			return FileVisitResult.CONTINUE;
		}
	}


//	class CopyFileVisitor extends SimpleFileVisitor<Path> {
//	
//	private Path source = null;
//	private final Path target;
//	
//
//	public CopyFileVisitor(Path target) {
//		this.target = target;
//	}
//
//	@Override
//	public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
//		
//		System.err.println("pre visit directory - source: " + source);
//		System.err.println("pre visit directory - dir: " + dir);
//		
//		if (source == null) {
//			source = dir;
//		} else {
//			Files.createDirectories(target.resolve(source.relativize(dir)));
//		}
//		return FileVisitResult.CONTINUE;
//	}
//
//	@Override
//	public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
//		Files.copy(file, target.resolve(source.relativize(file)));
//		return FileVisitResult.CONTINUE;
//	}
//}
}
