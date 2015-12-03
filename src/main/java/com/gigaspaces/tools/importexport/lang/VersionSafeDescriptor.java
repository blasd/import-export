package com.gigaspaces.tools.importexport.lang;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.*;
import com.gigaspaces.metadata.index.SpaceIndex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by skyler on 12/2/2015.
 */
public class VersionSafeDescriptor implements Serializable {
    private static final long serialVersionUID = 8226379400049132260L;
    private static final String PROPERTY_TYPE = "__PROPERTY_TYPE";
    private static final String STORAGE_TYPE = "__STORAGE_TYPE";
    private static final String DOCUMENT_SUPPORT = "__DOCUMENT_TYPE";
    private String typeName;
    private FifoSupport fifoSupport;
    private StorageType storageType;
    private boolean supportsOptimisticLocking;
    private String routingPropertyName;
    private String idPropertyName;
    private boolean supportsDynamicProperties;
    private Map<String, SpaceIndex> indexes;
    private Class<? extends SpaceDocument> documentWrapperClass;
    private Class<? extends Object> objectClass;
    private Map<String, Map<String, Object>> properties;
    private boolean isAutoGeneratedId;

    public VersionSafeDescriptor(){
        indexes = new HashMap<>();
        properties = new HashMap<>();
    }

    public static VersionSafeDescriptor create(SpaceTypeDescriptor typeDescriptor) {
        VersionSafeDescriptor output = new VersionSafeDescriptor();

        output.setTypeName(typeDescriptor.getTypeName());
        output.setFifoSupport(typeDescriptor.getFifoSupport());
        output.setStorageType(typeDescriptor.getStorageType());
        output.setSupportsDynamicProperties(typeDescriptor.supportsDynamicProperties());
        output.setSupportsOptimisticLocking(typeDescriptor.supportsOptimisticLocking());
        output.setIdPropertyName(typeDescriptor.getIdPropertyName());
        output.setRoutingPropertyName(typeDescriptor.getRoutingPropertyName());
        output.setIndexes(typeDescriptor.getIndexes());
        output.setDocumentWrapperClass(typeDescriptor.getDocumentWrapperClass());
        output.setObjectClass(typeDescriptor.getObjectClass());
        output.setIsAutoGeneratedId(typeDescriptor.isAutoGenerateId());

        for (int x = 0; x < typeDescriptor.getNumOfFixedProperties(); x++) {
            SpacePropertyDescriptor fixedProperty = typeDescriptor.getFixedProperty(x);
            output.addProperty(fixedProperty.getName(), fixedProperty.getType(), fixedProperty.getStorageType(), fixedProperty.getDocumentSupport());
        }

        return output;
    }

    private void addProperty(String name, Class<?> type, StorageType storageType, SpaceDocumentSupport documentSupport) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(PROPERTY_TYPE, type);
        metadata.put(STORAGE_TYPE, storageType);
        metadata.put(DOCUMENT_SUPPORT, documentSupport);

        this.properties.put(name, metadata);
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setFifoSupport(FifoSupport fifoSupport) {
        this.fifoSupport = fifoSupport;
    }

    public FifoSupport getFifoSupport() {
        return fifoSupport;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setSupportsOptimisticLocking(boolean supportsOptimisticLocking) {
        this.supportsOptimisticLocking = supportsOptimisticLocking;
    }

    public boolean getSupportsOptimisticLocking() {
        return supportsOptimisticLocking;
    }

    public void setRoutingPropertyName(String routingPropertyName) {
        this.routingPropertyName = routingPropertyName;
    }

    public String getRoutingPropertyName() {
        return routingPropertyName;
    }

    public void setIdPropertyName(String idPropertyName) {
        this.idPropertyName = idPropertyName;
    }

    public String getIdPropertyName() {
        return idPropertyName;
    }

    public void setSupportsDynamicProperties(boolean supportsDynamicProperties) {
        this.supportsDynamicProperties = supportsDynamicProperties;
    }

    public boolean getSupportsDynamicProperties() {
        return supportsDynamicProperties;
    }

    public void setIndexes(Map<String,SpaceIndex> indexes) {
        this.indexes = indexes;
    }

    public Map<String, SpaceIndex> getIndexes() {
        return indexes;
    }

    public void setDocumentWrapperClass(Class<? extends SpaceDocument> documentWrapperClass) {
        this.documentWrapperClass = documentWrapperClass;
    }

    public Class<? extends SpaceDocument> getDocumentWrapperClass() {
        return documentWrapperClass;
    }

    public void setObjectClass(Class<? extends Object> objectClass) {
        this.objectClass = objectClass;
    }

    public Class<? extends Object> getObjectClass() {
        return objectClass;
    }

    public SpaceTypeDescriptor toSpaceTypeDescriptor() {
        SpaceTypeDescriptorBuilder builder;

        if (this.getObjectClass() != null)
            builder = new SpaceTypeDescriptorBuilder(this.getObjectClass(), null);
        else {
            builder = new SpaceTypeDescriptorBuilder(getTypeName());

            if (getDocumentWrapperClass() != null)
                builder.documentWrapperClass(getDocumentWrapperClass());

            builder.supportsDynamicProperties(this.getSupportsDynamicProperties());
            builder.supportsOptimisticLocking(this.getSupportsOptimisticLocking());

            if (getFifoSupport() != null)
                builder.fifoSupport(this.getFifoSupport());

            if (this.getStorageType() != null)
                builder.storageType(this.getStorageType());

            builder.idProperty(getIdPropertyName(), getIsAutoGeneratedId());
            builder.routingProperty(getRoutingPropertyName());

            for (Map.Entry<String, Map<String, Object>> property : getProperties().entrySet()) {
                Object propertyType = property.getValue().get(PROPERTY_TYPE);
                StorageType storageType = (StorageType) property.getValue().get(STORAGE_TYPE);
                SpaceDocumentSupport documentSupport = (SpaceDocumentSupport) property.getValue().get(DOCUMENT_SUPPORT);

                builder.addFixedProperty(property.getKey(), (Class)propertyType,(documentSupport == null ? SpaceDocumentSupport.DEFAULT : documentSupport),(storageType == null ? StorageType.OBJECT : storageType));
            }
        }

        return builder.create();
    }

    public void setIsAutoGeneratedId(boolean isAutoGeneratedId) {
        this.isAutoGeneratedId = isAutoGeneratedId;
    }

    public boolean getIsAutoGeneratedId() {
        return isAutoGeneratedId;
    }

    public Map<String,Map<String,Object>> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Map<String, Object>> properties) {
        this.properties = properties;
    }
}
