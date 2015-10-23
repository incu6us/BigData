package com.incu6us.openstack.swiftstorage;

/**
 * Created by vpryimak on 22.10.2015.
 */

import com.incu6us.openstack.utils.PropertyUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.openstack4j.api.OSClient;
import org.openstack4j.core.transport.internal.HttpLoggingFilter;
import org.openstack4j.model.common.DLPayload;
import org.openstack4j.model.common.Payload;
import org.openstack4j.model.storage.object.SwiftContainer;
import org.openstack4j.model.storage.object.SwiftObject;
import org.openstack4j.model.storage.object.options.ObjectLocation;
import org.openstack4j.openstack.OSFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Created by vpryimak on 19.10.2015.
 */

public class SwiftStorage {

    private static Logger LOGGER = LoggerFactory.getLogger(SwiftStorage.class);

    private OSClient client = null;
    private PropertyUtil util = null;
    private Boolean debug = false;

    /**
     * search in resource-folder for "swift.properties"
     * With property file, which must contain:
     * debug=false
     * endpoint=https://exaple.com:5000/v2.0
     * username=admin
     * password=p@ssw0rd
     * tenant=TENANT01
     */
    public SwiftStorage() {
        util = new PropertyUtil();
        init();
    }

    /**
     * With property file, which must contain:
     * debug=false
     * endpoint=https://exaple.com:5000/v2.0
     * username=admin
     * password=p@ssw0rd
     * tenant=TENANT01
     *
     * @param prefixDirConf for path in resource dir
     */
    public SwiftStorage(String prefixDirConf) {
        util = new PropertyUtil(prefixDirConf);
        init();
    }

    /**
     * With property file, which must contain:
     * debug=false
     * endpoint=https://exaple.com:5000/v2.0
     * username=admin
     * password=p@ssw0rd
     * tenant=TENANT01
     *
     * @param prefixDirConf for path in resource dir
     * @param confFile      name for new file name (against "swift.properties")
     */
    public SwiftStorage(String prefixDirConf, String confFile) {
        util = new PropertyUtil(prefixDirConf, confFile);
        init();
    }

    /**
     * Will list all containers in Object Storage
     *
     * @return - List<SwiftContainer> list of Swift containers
     */
    public List<SwiftContainer> listAllContainers() {
        List<SwiftContainer> listObj = new ArrayList<SwiftContainer>();
        listObj.addAll(client.objectStorage().containers().list());
        return listObj;
    }

    /**
     * List objects from container
     *
     * @param containerName - container name for listing objects
     * @return
     */
    public List<SwiftObject> listObjectsFromContainer(String containerName) {
        List<SwiftObject> listObj = new ArrayList<SwiftObject>();
        listObj.addAll(client.objectStorage().objects().list(containerName));
        return listObj;
    }

    /**
     * Find similar objects in container
     *
     * @param container  - container name
     * @param objectName - the part of object name
     * @return
     */
    public List<SwiftObject> listObjectsFromContainerLike(String container, final String objectName) {
        List<SwiftObject> listObj = new ArrayList<SwiftObject>();
        Predicate condition = new Predicate() {
            public boolean evaluate(Object object) {
                return ((SwiftObject) object).getName().matches(Pattern.compile(".*" + objectName + ".*").pattern());
            }
        };

        listObj.addAll(CollectionUtils.select(client.objectStorage().objects().list(container), condition));

        return listObj;
    }


    /**
     * Copy object or pseudo-directory to local file system
     *
     * @param container  - container name
     * @param objectName - object name
     * @param localPath  - path on local file system
     */
    public void copyToLocalFs(String container, String objectName, String localPath) {
        ObjectLocation oLocation = ObjectLocation.create(container, objectName); //.replaceAll("(.*)(//)$", "$1"));
        SwiftObject sObject = client.objectStorage().objects().get(oLocation);

        try {

            // copy only one file (object)
            DLPayload payload = sObject.download();

            try {
                payload.writeToFile(new File(localPath + systemSeparator() + sObject.getName().replaceAll("^(.*)/(.*)$", "$2")));
            } catch (IOException e) {
                LOGGER.error("IOException: " + e);
            }

            // if object exist
        } catch (NullPointerException e) {
            LOGGER.info("copy for dir");

            // copy all dir
            for (SwiftObject o : listObjectsFromContainerLike(container, objectName)) {
                DLPayload payload = o.download();

                try {

                    // make current path for different OS's (win/linux)
                    String fullUrl = localPath + systemSeparator() + o.getName().replaceAll("^(.*)/(.*)$", "$1").replaceAll("\\/", systemSeparator());
                    File dir = new File(fullUrl);

                    // create dir if not exist
                    if (!dir.isDirectory() && !dir.exists()) {
                        LOGGER.info("Making dir: " + fullUrl);
                        dir.mkdirs();
                    }

                    LOGGER.info("processing: " + o.getName() + " ...");
                    payload.writeToFile(new File(fullUrl + systemSeparator() + o.getName().replaceAll("^(.*)/(.*)$", "$2")));
                } catch (IOException e1) {
                    LOGGER.error("IOException: " + e1);
                }
            }
            LOGGER.info("Done");
        }
    }

    /**
     * Write stream to Swift Storage
     * <p/>
     * Example for payload data: Payloads.create(new File("/path/to/file.txt"))
     *
     * @param container
     * @param objectName
     * @param data
     */
    public void writeToStorage(String container, String objectName, Payload data) {
        LOGGER.info("Write: " + objectName + " - to container: " + container + " ...");
        client.objectStorage().objects().put(container, objectName, data);
        LOGGER.info("Data was written successfully!");
    }

    /**
     * Read an object from Swift Storage
     *
     * @param container
     * @param objectName
     * @return
     */
    public InputStream readFromStorage(String container, String objectName) {
        ObjectLocation oLocation = ObjectLocation.create(container, objectName);
        SwiftObject sObject = client.objectStorage().objects().get(oLocation);
        DLPayload payload = null;

        try {
            payload = sObject.download();
        } catch (NullPointerException e) {
            LOGGER.error("Stream is null for file: " + sObject + " ... " + e);
        }

        return payload.getInputStream();
    }

    /**
     * Copy objects internally in SwiftStorrage
     *
     * @param container
     * @param objectName
     * @param newObjectName
     */
    public void copyToStorage(String container, String objectName, String newObjectName) {
        ObjectLocation oLocation = ObjectLocation.create(container, objectName);
        ObjectLocation newLocation = ObjectLocation.create(container, newObjectName);

        LOGGER.info("Copy an object... src: " + oLocation.getObjectName() + " dest: " + newLocation.getObjectName());

        client.objectStorage().objects().copy(oLocation, newLocation);
    }

    /**
     * Delete an object internally in Swift Storage
     *
     * @param container
     * @param objectName
     */
    public void deleteObjectFromStorage(String container, String objectName) {
        ObjectLocation oLocation = ObjectLocation.create(container, objectName);

        LOGGER.info("Delete an object: " + oLocation.getObjectName());

        client.objectStorage().objects().delete(oLocation);
    }

    /**
     * System separator which is different for Windows and *Unix systems
     *
     * @return
     */
    private String systemSeparator() {
        String os = System.getProperty("os.name").toLowerCase().substring(0, 3);

        if (os.equals("win")) {
            return "\\\\";
        }

        return "/";
    }

    /**
     * Init method
     */
    private void init() {
        // For debug output
        debug = Boolean.parseBoolean(util.getProperties().getProperty("debug"));
        LOGGER.debug("HTTP Debug: " + debug);

        OSFactory.enableHttpLoggingFilter(debug);

        LOGGER.debug("Endpoint: " + util.getProperties().getProperty("endpoint"));
        LOGGER.debug("Username: " + util.getProperties().getProperty("username"));
        LOGGER.debug("Password: " + util.getProperties().getProperty("password"));
        LOGGER.debug("Tenant: " + util.getProperties().getProperty("tenant"));

        try {
            client = OSFactory.builder()
                    .endpoint(util.getProperties().getProperty("endpoint"))
                    .credentials(util.getProperties().getProperty("username"), util.getProperties().getProperty("password"))
                    .tenantName(util.getProperties().getProperty("tenant")).authenticate();
        } catch (AbstractMethodError e) {
            LOGGER.error("Client Builder failed: " + e);
        }
        LOGGER.debug("HTTP debug: " + System.getProperties().getProperty(HttpLoggingFilter.class.getName()));
        LOGGER.debug("Endpoint: " + client.getEndpoint());
    }
}