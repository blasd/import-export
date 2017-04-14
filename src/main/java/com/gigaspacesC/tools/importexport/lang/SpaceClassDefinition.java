package com.gigaspacesC.tools.importexport.lang;

import java.io.Serializable;
import java.util.HashMap;

import org.openspaces.core.GigaSpace;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.google.common.base.Function;

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

	public static SpaceClassDefinition create(final GigaSpace space,
			ExportConfiguration configuration,
			String className) {
		Function<String, SpaceTypeDescriptor> typeProvider = new ClassNameToSpaceTypeDescriptor(space.getTypeManager()) ;

		// Get current class SpaceTypeDescriptor
		SpaceTypeDescriptor unsafe = typeProvider.apply(className);

		// Compute the VersionSafeDescriptor for current class, and its parent classes
		VersionSafeDescriptor versionSafeDescriptor = VersionSafeDescriptor.create(unsafe, typeProvider);

		final SpaceClassDefinition output;
		if (!unsafe.isConcreteType() || configuration.isJarLess()) {
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

