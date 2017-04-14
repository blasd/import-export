package com.gigaspacesC.tools.importexport.lang;

import org.openspaces.core.GigaSpaceTypeManager;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.google.common.base.Function;

public class ClassNameToSpaceTypeDescriptor implements Function<String, SpaceTypeDescriptor> {

	private final GigaSpaceTypeManager typeManager;

	public ClassNameToSpaceTypeDescriptor(GigaSpaceTypeManager typeManager) {
		this.typeManager = typeManager;
	}

	@Override
	public SpaceTypeDescriptor apply(String someClass) {
		return typeManager.getTypeDescriptor(someClass);
	}

}
