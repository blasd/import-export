package com.gigaspacesC.tools.importexport.lang;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.internal.utils.ReflectionUtils.FieldCallback;
import com.gigaspaces.internal.utils.ReflectionUtils.FieldFilter;
import com.j_spaces.core.client.SQLQuery;

/**
 * Created by skyler on 12/1/2015.
 */
public class JavaClassDefinition extends SpaceClassDefinition implements Serializable {
	public final class ToMapFieldFilter implements FieldFilter {
		@Override
		public boolean matches(Field field) {
			return !Modifier.isStatic(field.getModifiers());
		}
	}

    public final class ToMapFieldCallBack implements FieldCallback {
		private final Object instance;
		private final HashMap<String, Object> output;

		private ToMapFieldCallBack(Object instance, HashMap<String, Object> output) {
			this.instance = instance;
			this.output = output;
		}

		@Override
		public void doWith(Field currentField) throws IllegalArgumentException, IllegalAccessException {
			ReflectionUtils.makeAccessible(currentField);

			output.put(currentField.getName(), currentField.get(instance));
		}
	}

	public final class ToInstanceFieldFilter implements FieldFilter {
		private final Entry<String, Object> property;

		private ToInstanceFieldFilter(Entry<String, Object> property) {
			this.property = property;
		}

		@Override
		public boolean matches(Field field) {
			return field.getName().equals(property.getKey()) && !Modifier.isStatic(field.getModifiers());
		}
	}

	public final class ToInstanceFieldCallBack implements FieldCallback {
		private final Object output;
		private final Entry<String, Object> property;

		private ToInstanceFieldCallBack(Object output, Entry<String, Object> property) {
			this.output = output;
			this.property = property;
		}

		@Override
		public void doWith(Field currentField) throws IllegalArgumentException, IllegalAccessException {
			ReflectionUtils.makeAccessible(currentField);

			try {
				currentField.set(output, property.getValue());
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	private static final long serialVersionUID = 4910571443452951970L;
    private transient Field routingField;
	private transient Method routingGetter;

    public JavaClassDefinition(String className, VersionSafeDescriptor typeDescriptor){
        super(className, typeDescriptor);
    }

    @Override
    public  Object toTemplate() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return new SQLQuery(Class.forName(this.className), "");
    }

	@Override
	public HashMap<String, Object> toMap(final Object instance) throws ClassNotFoundException, IllegalAccessException {
		final HashMap<String, Object> output = new HashMap<String, Object>();

		Class<?> aClass = Class.forName(className);

		// Iterate through all fields, including parent classes
		ReflectionUtils.doWithFields(aClass, new ToMapFieldCallBack(instance, output), new ToMapFieldFilter());

		return output;
	}

    private boolean canBeSet(Field currentField) {
        return !Modifier.isFinal(currentField.getModifiers());
    }

    @Override
	public Object getRoutingValue(Object instance)
			throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
		String routingPropertyName = typeDescriptor.getRoutingPropertyName();

		if (this.routingField == null && this.routingGetter == null) {
			Class<?> aClass = Class.forName(className);

			try {
				routingField = aClass.getDeclaredField(routingPropertyName);
				routingField.setAccessible(true);
			} catch (NoSuchFieldException e) {
				// The routingPropertyName is not a field. It may be a JavaBean setter

				// http://stackoverflow.com/questions/5503412/java-standard-library-to-convert-field-name-firstname-to-accessor-method-na
				try {
					String methodName = "get" + routingPropertyName.substring(0, 1).toUpperCase()
							+ routingPropertyName.substring(1);
					routingGetter = org.springframework.util.ReflectionUtils.findMethod(aClass, methodName);
				} catch (SecurityException e1) {
					throw new RuntimeException(e1);
				}
			}
		}

		if (routingField != null) {
			return routingField.get(instance);
		} else if (routingGetter != null) {
			try {
				return routingGetter.invoke(instance);
			} catch (IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new RuntimeException("Found n oway to introspect " + className + " for field " + routingPropertyName);
		}
	}

	@Override
	public Object toInstance(HashMap<String, Object> asMap)
			throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException {
		Class<?> aClass = Class.forName(className);
		final Object output = aClass.newInstance();

		for (final Map.Entry<String, Object> property : asMap.entrySet()) {
			// Iterate through all fields, including parent classes
			ReflectionUtils.doWithFields(aClass, new ToInstanceFieldCallBack(output, property), new ToInstanceFieldFilter(property));
		}

		return output;
	}
}
