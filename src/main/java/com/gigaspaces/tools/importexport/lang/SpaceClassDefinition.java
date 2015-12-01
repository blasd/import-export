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
    protected SpaceTypeDescriptor typeDescriptor;


    public static SpaceClassDefinition create(GigaSpace space, String className){
        SpaceClassDefinition output;
        SpaceTypeDescriptor typeDescriptor = space.getTypeManager().getTypeDescriptor(className);

        if(!typeDescriptor.isConcreteType()){
            output = new DocumentClassDefinition(className, typeDescriptor);
        } else {
            output = new JavaClassDefinition(className, typeDescriptor);
        }

        return output;
    }

    public abstract HashMap<String, Object> toMap(Object instance) throws ClassNotFoundException, IllegalAccessException;

    public abstract Object toTemplate() throws ClassNotFoundException, InstantiationException, IllegalAccessException;

    public abstract Object getRoutingValue(Object instance) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException;
}

