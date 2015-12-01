package com.gigaspaces.tools.importexport.lang;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DocumentClassDefinition extends SpaceClassDefinition implements Serializable {

    private static final long serialVersionUID = -1095159043221713221L;

    public DocumentClassDefinition(String className, SpaceTypeDescriptor typeDescriptor) {
        this.className = className;
        this.typeDescriptor = typeDescriptor;
    }

    @Override
    public  Object toTemplate() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return new SpaceDocument(this.className);
    }

    @Override
    public HashMap<String, Object> toMap(Object instance) throws ClassNotFoundException, IllegalAccessException {
        HashMap<String, Object> output = new HashMap<>();

        SpaceDocument asDocument = (SpaceDocument) instance;
        Map<String, Object> properties = asDocument.getProperties();

        for(Map.Entry<String, Object> kvp : properties.entrySet()){
            output.put(kvp.getKey(), kvp.getValue());
        }

        return output;
    }

    @Override
    public Object getRoutingValue(Object instance) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        String routingPropertyName = typeDescriptor.getRoutingPropertyName();
        return ((SpaceDocument) instance).getProperty(routingPropertyName);
    }
}
