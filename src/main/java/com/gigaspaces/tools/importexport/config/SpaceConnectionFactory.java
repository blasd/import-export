package com.gigaspaces.tools.importexport.config;

import com.google.common.base.Joiner;
import com.j_spaces.core.IJSpace;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.List;
import java.util.logging.Logger;

import static com.google.common.base.Strings.isNullOrEmpty;

public class SpaceConnectionFactory {

    private static Logger logger = Logger.getLogger(SpaceConnectionFactory.class.getName());
    private static final int LOOKUP_TIMEOUT = 2000;

    private ExportConfiguration config;
    private UrlSpaceConfigurer urlSpaceConfigurer;
    private GigaSpace space;
    private Admin admin;

    public SpaceConnectionFactory(ExportConfiguration config) {
        this.config = config;
    }

    @Deprecated
    public String buildSpaceURL(){


        return "";
    }

    public GigaSpace space() {

        if(space == null) {
            urlSpaceConfigurer = new UrlSpaceConfigurer("jini://*/*/" + config.getName());
            urlSpaceConfigurer.lookupLocators(join(config.getLocators()));
            urlSpaceConfigurer.lookupGroups(join(config.getGroups()));

            if (!isNullOrEmpty(config.getUsername()) && !isNullOrEmpty(config.getPassword())) {
                logger.info("Using secured connection.");
                urlSpaceConfigurer.credentials(config.getUsername(), config.getPassword());
            }

            GigaSpaceConfigurer gigaSpaceConfigurer = new GigaSpaceConfigurer(urlSpaceConfigurer.lookupTimeout(LOOKUP_TIMEOUT));

            space = gigaSpaceConfigurer.create();

            IJSpace space1 = space.getSpace();
            logger.info("Creating connection with url: \n" + space1.getURL());
        }

        return space;
    }

    private String join(List<String> args) {
        return Joiner.on(",").join(args);
    }

    public void close() throws Exception {
        if(urlSpaceConfigurer != null){
            urlSpaceConfigurer.destroy();
        }

        if(admin != null){
            admin.close();
        }
    }

    public Admin createAdmin() {
        if(admin == null){
            AdminFactory adminFactory = new AdminFactory();
            adminFactory.addGroups(join(config.getGroups()));
            adminFactory.addLocators(join(config.getLocators()));

            if (!isNullOrEmpty(config.getUsername()) && !isNullOrEmpty(config.getPassword())) {
                logger.info("Using secured admin connection.");
                adminFactory.credentials(config.getUsername(), config.getPassword());
            }

            admin = adminFactory.create();
        }

        return admin;
    }
}
