package org.apache.nutch.analysis.lang;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ProfileLoader {
	public List<String> profileLoader() throws IOException {
        String dirname = "profiles/";
        Enumeration<URL> en = com.cybozu.labs.langdetect.Detector.class.getClassLoader().getResources(
                dirname);
        List<String> profiles = new ArrayList<>();
        if (en.hasMoreElements()) {
            URL url = en.nextElement();
            JarURLConnection urlcon = (JarURLConnection) url.openConnection();
            try (JarFile jar = urlcon.getJarFile();) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    String entry = entries.nextElement().getName();
                    if (entry.startsWith(dirname)) {
                        try (InputStream in = com.cybozu.labs.langdetect.Detector.class.getClassLoader()
                                .getResourceAsStream(entry);) {
                            profiles.add(IOUtils.toString(in));
                        }
                    }
                }
            }
        }
        return profiles;
    }

}
