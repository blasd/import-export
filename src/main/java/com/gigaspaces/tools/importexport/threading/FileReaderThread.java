package com.gigaspaces.tools.importexport.threading;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.OptionalDataException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.openspaces.core.GigaSpace;

import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.io.CustomObjectInputStream;
import com.gigaspaces.tools.importexport.lang.SpaceClassDefinition;
import com.gigaspaces.tools.importexport.lang.VersionSafeDescriptor;
import com.gigaspaces.tools.importexport.remoting.FileImportTask;
import com.google.common.io.CountingInputStream;

/**
 * Created by skyler on 12/1/2015.
 */
public class FileReaderThread implements Callable<ThreadAudit> {
	private static final Logger LOGGER = Logger.getLogger(FileReaderThread.class.getName());

	private final GigaSpace space;
	private final ExportConfiguration config;
	private final String className;
	private final String fileName;

	public FileReaderThread(GigaSpace space, ExportConfiguration config, String className, String fileName) {
		this.space = space;
		this.config = config;
		this.className = className;
		this.fileName = fileName;
	}

	@Override
	public ThreadAudit call() throws Exception {
		ThreadAudit output = new ThreadAudit(fileName);
		output.start();

		try {
			String fullFilePath = getFullFilePath();
			File asFile = new File(fullFilePath);
			CountingInputStream counting = new CountingInputStream(new FileInputStream(fullFilePath));
			GZIPInputStream zis = new GZIPInputStream(new BufferedInputStream(counting));
			CustomObjectInputStream input = new CustomObjectInputStream(zis);

			int recordCount = 0;

			try {
				validateFileNameAndType(input.readUTF());
				Object readObject = input.readObject();
				if (readObject instanceof SpaceClassDefinition) {
					SpaceClassDefinition classDefinition = (SpaceClassDefinition) readObject;

					VersionSafeDescriptor typeDescriptor = classDefinition.getTypeDescriptor();

					while (typeDescriptor != null) {
						// We need to register all parent classes to enable export on imported POJO
						space.getTypeManager().registerTypeDescriptor(typeDescriptor.toSpaceTypeDescriptor());
						typeDescriptor = typeDescriptor.getSuperVersionSafeDescriptor();
					}

					Collection<Object> spaceInstances = new ArrayList<Object>();

					Object record;

					do {
						record = tryReadNextObject(input);

						if (record != null) {
							spaceInstances.add(classDefinition.toInstance((HashMap<String, Object>) record));
							recordCount++;

							if (recordCount % 1000 == 0) {
								LOGGER.info("We have read " + recordCount
										+ " records for "
										+ counting.getCount()
										+ "bytes out of "
										+ asFile.length()
										+ " for "
										+ asFile);
							}
						}

						if ((spaceInstances.size() > 0 && (spaceInstances.size() % config.getBatch() == 0))
								|| (spaceInstances.size() > 0 && record == null)) {
							flush(spaceInstances);
						}
					} while (record != null);
				}

				output.setCount(recordCount);
			} finally {
				input.close();
				zis.close();
			}

			LOGGER.info("We have successfully read " + recordCount
					+ " records for "
					+ counting.getCount()
					+ " bytes for "
					+ asFile);
		} catch (Exception ex) {
			LOGGER.log(Level.WARNING, "Issue when loading " + fileName, ex);
			output.setException(ex);
		}

		output.stop();
		return output;
	}

	private Object tryReadNextObject(CustomObjectInputStream input) throws Exception {
		Object output = null;
		try {
			output = input.readObject();
		} catch (EOFException ex) {

		} catch (OptionalDataException ex) {

		}

		return output;
	}

	private void validateFileNameAndType(String className) {
		if (!className.equals(this.className)) {
			throw new SecurityException(String.format(
					"File name prefix does not match the encoded class name (case-sensitive). File name (prefix): [%s] Serialized class name: [%s]",
					this.className,
					className));
		}
	}

	private void flush(Collection<Object> spaceInstances) {
		space.writeMultiple(spaceInstances.toArray(), WriteModifiers.ONE_WAY);
		spaceInstances.clear();
	}

	public String getFullFilePath() {
		return config.getDirectory() + File.separator + fileName;
	}
}
