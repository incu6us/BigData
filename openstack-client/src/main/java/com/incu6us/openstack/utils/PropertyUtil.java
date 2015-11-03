package com.incu6us.openstack.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by vpryimak on 22.10.2015.
 */
public class PropertyUtil {

    final static Logger LOGGER = LoggerFactory.getLogger(PropertyUtil.class);

    private String prefixDir = null;
    private Properties prop = null;
    private String confFile = null;
    private InputStream is = null;

    /**
     * Constructor without prefix directory
     * </p>
     * Will search in resource-folder for "swift.properties"
     */
    public PropertyUtil() {
        prefixDir = "";
        confFile = "swift.properties";
        loadProperties();
    }

    public PropertyUtil(Properties prop) {
        this.prop = prop;
    }

    /**
     * @param prefixDir
     */
    public PropertyUtil(String prefixDir) {
        this.prefixDir = prefixDir;
        loadProperties();
    }


    public PropertyUtil(String prefixDir, String confFile) {
        this.prefixDir = prefixDir;
        this.confFile = confFile;
        loadProperties();
    }

    /**
     * Read property file, which must contain:
     * debug=false
     * endpoint=https://exaple.com:5000/v2.0
     * username=admin
     * password=p@ssw0rd
     * tenant=TENANT01
     *
     * @return
     */
    public Properties getProperties() {
        return prop;
    }

    /**
     * Load properties
     */
    private void loadProperties() {
        prop = new Properties();
        String propertyFileName = null;
        if (prefixDir.isEmpty()) {
            propertyFileName = confFile;
        } else {
            propertyFileName = prefixDir + "/" + confFile;
        }

        LOGGER.debug("Property file: " + propertyFileName);

        is = getClass().getClassLoader().getResourceAsStream(propertyFileName);
        LOGGER.info(getClass().getClassLoader().toString());
        try {
            prop.load(is);
        } catch (IOException e) {
            LOGGER.error("Cannot read config file from: " + propertyFileName + " " + e);
        }
    }
}
