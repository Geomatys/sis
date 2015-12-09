/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sis.internal.doclet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.formats.html.HtmlDoclet;


/**
 * A doclet which delegates the work to the standard doclet, then performs additional actions.
 * The post-javadoc actions are:
 *
 * <ul>
 *   <li>Rename {@code "stylesheet.css"} as {@code "standarc.css"}.</li>
 *   <li>Copy {@code "src/main/javadoc/stylesheet.css"} (from the project root) to {@code "stylesheet.css"}.</li>
 *   <li>Copy additional resources.</li>
 * </ul>
 *
 * We do not use the standard {@code "-stylesheet"} Javadoc option because it replaces the standard
 * CSS by the specified one. Instead, we want to keep both the standard CSS and our customized one.
 * Our customized CSS shall contain an import statement for the standard stylesheet.
 *
 * <p>This class presumes that all CSS files are encoded in UTF-8.</p>
 *
 * @author  Martin Desruisseaux (Geomatys)
 * @since   0.5
 * @version 0.5
 * @module
 */
public final class Doclet extends HtmlDoclet {
    /**
     * The name of the standard stylesheet file generated by Javadoc.
     */
    private static final String STYLESHEET = "stylesheet.css";

    /**
     * The name of the standard stylesheet after renaming by this doclet.
     */
    private static final String RENAMED_CSS = "standard.css";

    /**
     * Path to the custom CSS (relative to the project home).
     */
    private static final String CUSTOM_CSS = "src/main/javadoc/" + STYLESHEET;

    /**
     * The encoding to use for reading and writing CSS files.
     */
    private static final String ENCODING = "UTF-8";

    /**
     * Invoked by Javadoc for starting the doclet.
     *
     * @param  root The root document.
     * @return {@code true} on success, or {@code false} on failure.
     */
    public static boolean start(final RootDoc root) {
        String outputDirectory = null;
        for (final String[] option : root.options()) {
            if (option.length == 2) {
                if ("-d".equals(option[0])) {
                    outputDirectory = option[1];
                }
            }
        }
        final boolean success = HtmlDoclet.start(root);
        if (success && outputDirectory != null) try {
            final File output = new File(outputDirectory);
            final File customCSS = customCSS(output);
            copyStylesheet(customCSS, output);
            copyResources(customCSS.getParentFile(), output);
            final Rewriter r = new Rewriter();
            for (final File file : output.listFiles()) {
                if (file.isDirectory()) {  // Do not process files in the root directory, only in sub-directories.
                    r.processDirectory(file);
                }
            }
        } catch (IOException e) {
            final StringWriter buffer = new StringWriter();
            final PrintWriter p = new PrintWriter(buffer);
            e.printStackTrace(p);
            root.printError(buffer.toString());
            return false;
        }
        return success;
    }

    /**
     * Scans parents of the given directory until we find the root of the Maven project and the custom CSS file.
     */
    private static File customCSS(File directory) throws FileNotFoundException {
        boolean isModuleDirectory = false;
        while ((directory = directory.getParentFile()) != null && directory.canRead()) {
            if (new File(directory, "pom.xml").isFile()) {
                isModuleDirectory = true;
                final File candidate = new File(directory, CUSTOM_CSS);
                if (candidate.exists()) {
                    return candidate;
                }
            } else if (isModuleDirectory) {
                // If we were in a Maven module and the parent directory does not
                // have a pom.xml file, then we are no longer in the Maven project.
                break;
            }
        }
        throw new FileNotFoundException("Can not locate " + CUSTOM_CSS + " from the root of this Maven project.");
    }

    /**
     * Opens a CSS file for reading.
     */
    private static BufferedReader openReader(final File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file), ENCODING));
    }

    /**
     * Opens a CSS file for writing.
     */
    private static BufferedWriter openWriter(final File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), ENCODING));
    }

    /**
     * Copies the standard CSS file, then copies the custom CSS file.
     *
     * @param  inputFile        The custom CSS file to copy in the destination directory.
     * @param  outputDirectory  The directory where to copy the CSS file.
     * @throws IOException      If an error occurred while reading or writing.
     */
    private static void copyStylesheet(final File inputFile, final File outputDirectory) throws IOException {
        final File stylesheetFile = new File(outputDirectory, STYLESHEET);
        final File standardFile   = new File(outputDirectory, RENAMED_CSS);
        /*
         * Copy the standard CSS file, skipping the import of DejaVu font
         * since our custom CSS file does not use it.
         */
        try (final BufferedReader in  = openReader(stylesheetFile);
             final BufferedWriter out = openWriter(standardFile))
        {
            String line;
            while ((line = in.readLine()) != null) {
                if (!line.equals("@import url('resources/fonts/dejavu.css');")) {
                    out.write(line);
                    out.newLine();
                }
            }
        }
        /*
         * Copy the custom CSS file, skipping comments for more compact file.
         */
        try (final BufferedReader in  = openReader(inputFile);
             final BufferedWriter out = openWriter(stylesheetFile))
        {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.length() < 2 || line.charAt(1) != '*') {
                    out.write(line);
                    out.newLine();
                }
            }
        }
    }

    /**
     * Creates links to Javadoc resources in the top-level directory (not from "{@code doc-files}" subdirectories).
     * While the Maven documentation said that the "{@code src/main/javadoc}" directory is copied by default, or a
     * directory can be specified with {@code <javadocResourcesDirectory>}, I have been unable to make it work even
     * with absolute paths.
     *
     * @param  inputFile        The directory containing resources.
     * @param  outputDirectory  The directory where to copy the resource files.
     * @throws IOException      If an error occurred while reading or writing.
     */
    private static void copyResources(final File inputDirectory, final File outputDirectory) throws IOException {
        final File[] inputFiles = inputDirectory.listFiles((final File dir, final String name) ->
                !name.startsWith(".") && !name.equals("overview.html") && !name.equals(STYLESHEET));
        try {
            for (final File input : inputFiles) {
                final File output = new File(outputDirectory, input.getName());
                if (!output.exists()) { // For avoiding a failure if the target exists.
                    Files.createLink(output.toPath(), input.toPath());
                }
            }
        } catch (UnsupportedOperationException | FileSystemException e) {
            /*
             * If hard links are not supported, performs plain copy instead.
             */
            final byte[] buffer = new byte[4096];
            for (final File input : inputFiles) {
                try (final FileInputStream  in  = new FileInputStream(input);
                     final FileOutputStream out = new FileOutputStream(new File(outputDirectory, input.getName())))
                {
                    int c;
                    while ((c = in.read(buffer)) >= 0) {
                        out.write(buffer, 0, c);
                    }
                }
            }
        }
    }
}
