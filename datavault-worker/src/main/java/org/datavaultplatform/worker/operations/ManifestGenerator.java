package org.datavaultplatform.worker.operations;

import java.nio.file.Path;

public interface ManifestGenerator {

	String generate(Path bagDirectory, Path dataDirectory);
}