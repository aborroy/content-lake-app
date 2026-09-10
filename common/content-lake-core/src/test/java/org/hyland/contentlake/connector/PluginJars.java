package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorPlugin;

import org.springframework.core.io.Resource;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

/**
 * Builds real connector jars for the plugin-loading tests: compiles a source string against the SPI, jars
 * the classes with a {@code META-INF/services} entry, and writes it into a directory.
 *
 * <p>Real jars rather than a fake classloader, because the mechanism under test is exactly the part a mock
 * would skip: whether a class compiled outside this build, loaded from a file that was not on the
 * classpath, is discovered through {@link java.util.ServiceLoader} and resolves the SPI types to the host's
 * own. A test that hands the loader a pre-loaded class proves nothing about that.</p>
 */
final class PluginJars {

    static final String SERVICE_ENTRY = "META-INF/services/" + ConnectorPlugin.class.getName();

    private PluginJars() {
    }

    /**
     * Compiles {@code source} and writes it as a jar declaring {@code className} as a
     * {@link ConnectorPlugin}.
     *
     * @param directory where to write the jar
     * @param jarName   the jar's file name, which is also the origin the loader should report
     * @param className fully qualified name of the plugin class in {@code source}
     */
    static Path writePluginJar(Path directory, String jarName, String className, String source) {
        try {
            Path work = Files.createTempDirectory("plugin-build");
            Path sourceFile = work.resolve(className.substring(className.lastIndexOf('.') + 1) + ".java");
            Files.writeString(sourceFile, source, StandardCharsets.UTF_8);

            Path classes = Files.createDirectory(work.resolve("classes"));
            compile(sourceFile, classes);

            return writeJar(directory, jarName, classes, className);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A file that is named like a jar but is not one, which is what a truncated upload looks like. */
    static Path writeCorruptJar(Path directory, String jarName) {
        try {
            return Files.write(directory.resolve(jarName), "this is not a jar".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A well-formed jar whose service entry names a class it does not contain. */
    static Path writeJarDeclaringMissingClass(Path directory, String jarName, String className) {
        try (JarOutputStream jar = new JarOutputStream(
                Files.newOutputStream(directory.resolve(jarName)))) {
            jar.putNextEntry(new JarEntry(SERVICE_ENTRY));
            jar.write((className + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
            return directory.resolve(jarName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void compile(Path sourceFile, Path outputDirectory) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("No system Java compiler; run these tests on a JDK");
        }
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        int result = compiler.run(null, null, errors,
                "-classpath", pluginCompileClasspath(),
                "-d", outputDirectory.toString(),
                sourceFile.toString());
        if (result != 0) {
            throw new IllegalStateException("Could not compile the test plugin:\n"
                    + errors.toString(StandardCharsets.UTF_8));
        }
    }

    /**
     * What a connector compiles against: the SPI, plus spring-core for the {@code Resource} type
     * {@link org.hyland.contentlake.spi.ContentSourceClient} returns. Nothing else -- a plugin that needed
     * more of the host to compile would not be a plugin.
     *
     * <p>Located from the loaded classes rather than from {@code java.class.path}, because Surefire may hand
     * the JVM a manifest-only classpath jar, in which case that property does not name the directories the
     * compiler needs.</p>
     */
    private static String pluginCompileClasspath() {
        return Stream.of(ConnectorPlugin.class, Resource.class)
                .map(PluginJars::locationOf)
                .distinct()
                .collect(java.util.stream.Collectors.joining(File.pathSeparator));
    }

    private static String locationOf(Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        } catch (Exception e) {
            throw new IllegalStateException("Could not locate " + type.getName() + " for compilation", e);
        }
    }

    private static Path writeJar(Path directory, String jarName, Path classes, String serviceClassName)
            throws IOException {
        Path jarPath = directory.resolve(jarName);
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
            for (Path classFile : classFiles(classes)) {
                String entryName = classes.relativize(classFile).toString().replace('\\', '/');
                jar.putNextEntry(new JarEntry(entryName));
                jar.write(Files.readAllBytes(classFile));
                jar.closeEntry();
            }
            jar.putNextEntry(new JarEntry(SERVICE_ENTRY));
            jar.write((serviceClassName + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
        return jarPath;
    }

    private static List<Path> classFiles(Path classes) throws IOException {
        try (Stream<Path> walk = Files.walk(classes)) {
            List<Path> files = new ArrayList<>();
            walk.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".class"))
                    .forEach(files::add);
            return files;
        }
    }
}
