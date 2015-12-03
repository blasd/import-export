package com.gigaspaces.tools.importexport.lang;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by skyler on 11/30/2015.
 */
public  abstract class SpaceClassDefinition implements Serializable {
    private static final long serialVersionUID = 711154255702510496L;

    protected String className;
    protected VersionSafeDescriptor typeDescriptor;

    public SpaceClassDefinition(String className, VersionSafeDescriptor typeDescriptor){
        this.className = className;
        this.typeDescriptor = typeDescriptor;
    }

    public static SpaceClassDefinition create(GigaSpace space, String className){
        SpaceClassDefinition output;
        SpaceTypeDescriptor typeDescriptor = space.getTypeManager().getTypeDescriptor(className);
        VersionSafeDescriptor versionSafeDescriptor = VersionSafeDescriptor.create(typeDescriptor);

        if(!typeDescriptor.isConcreteType()){
            output = new DocumentClassDefinition(className, versionSafeDescriptor);
        } else {
            output = new JavaClassDefinition(className, versionSafeDescriptor);
        }

        return output;
    }

    public abstract HashMap<String, Object> toMap(Object instance) throws ClassNotFoundException, IllegalAccessException;

    public abstract Object toTemplate() throws ClassNotFoundException, InstantiationException, IllegalAccessException;

    public abstract Object getRoutingValue(Object instance) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException;

    public abstract Object toInstance(HashMap<String, Object> asMap) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException;

    public VersionSafeDescriptor getTypeDescriptor() {
        return this.typeDescriptor;
    }
}

