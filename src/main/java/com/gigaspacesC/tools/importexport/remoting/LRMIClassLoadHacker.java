package com.gigaspacesC.tools.importexport.remoting;

import java.io.Serializable;

import com.gigaspaces.tools.importexport.io.CustomObjectInputStream;
import com.gigaspaces.tools.importexport.lang.ClassNameToSpaceTypeDescriptor;
import com.gigaspaces.tools.importexport.lang.DocumentClassDefinition;
import com.gigaspaces.tools.importexport.lang.JavaClassDefinition;
import com.gigaspaces.tools.importexport.lang.SpaceClassDefinition;
import com.gigaspaces.tools.importexport.lang.VersionSafeDescriptor;
import com.gigaspaces.tools.importexport.lang.JavaClassDefinition.ToInstanceFieldCallBack;
import com.gigaspaces.tools.importexport.lang.JavaClassDefinition.ToInstanceFieldFilter;
import com.gigaspaces.tools.importexport.lang.JavaClassDefinition.ToMapFieldCallBack;
import com.gigaspaces.tools.importexport.lang.JavaClassDefinition.ToMapFieldFilter;
import com.gigaspaces.tools.importexport.threading.FileCreatorThread;
import com.gigaspaces.tools.importexport.threading.FileReaderThread;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;

/**
 * Created by skyler on 12/2/2015.
 */
public class LRMIClassLoadHacker implements Serializable {
    private static final long serialVersionUID = 7274957951824640690L;
    /** LRMI Class Loading Hack **/
    private FileCreatorThread creatorThread;
    private FileReaderThread readerThread;
    private ThreadAudit threadAudit;

    private SpaceClassDefinition spaceClassDefinition;
    private ClassNameToSpaceTypeDescriptor classNameToSpaceTypeDescriptor;
    private ToMapFieldFilter toMapFieldFilter;
    private ToMapFieldCallBack toMapFieldCallBack;
    private ToInstanceFieldFilter toInstanceFieldFilter;
    private ToInstanceFieldCallBack toInstanceFieldCallBack;
    
    private JavaClassDefinition javaClassDefinition;
    private DocumentClassDefinition documentClassDefinition;
    private VersionSafeDescriptor versionSafeDescriptor;
    private CustomObjectInputStream customObjectInputStream;

    /** LRMI Class Loading Hack **/
}
