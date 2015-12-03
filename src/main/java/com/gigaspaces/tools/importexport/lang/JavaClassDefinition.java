package com.gigaspaces.tools.importexport.lang;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by skyler on 12/1/2015.
 */
public class JavaClassDefinition extends SpaceClassDefinition implements Serializable {
    private static final long serialVersionUID = 4910571443452951970L;
    private transient Field routingField;

    public JavaClassDefinition(String className, VersionSafeDescriptor typeDescriptor){
        super(className, typeDescriptor);
    }

    @Override
    public  Object toTemplate() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return Class.forName(this.className).newInstance();
    }

    @Override
    public HashMap<String, Object> toMap(Object instance) throws ClassNotFoundException, IllegalAccessException {
        HashMap<String, Object> output = new HashMap<>();

        Class<?> aClass = Class.forName(className);
        Field[] fields = aClass.getDeclaredFields();

        for(Field currentField : fields){
            if(!canBeSet(currentField)) continue;

            currentField.setAccessible(true);
            output.put(currentField.getName(), currentField.get(instance));
        }

        return output;
    }

    private boolean canBeSet(Field currentField) {
        return !Modifier.isFinal(currentField.getModifiers());
    }

    @Override
    public Object getRoutingValue(Object instance) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        String routingPropertyName = typeDescriptor.getRoutingPropertyName();

        if(this.routingField == null) {
            Class<?> aClass = Class.forName(className);
            routingField = aClass.getDeclaredField(routingPropertyName);
            routingField.setAccessible(true);
        }

        return routingField.get(instance);
    }

    @Override
    public Object toInstance(HashMap<String, Object> asMap) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException {
        Class<?> aClass = Class.forName(className);
        Object output = aClass.newInstance();

        for(Map.Entry<String, Object> property : asMap.entrySet()){
            Field field = aClass.getDeclaredField(property.getKey());

            if(!canBeSet(field)) continue;

            field.setAccessible(true);
            field.set(output, property.getValue());
        }

        return output;
    }
}
