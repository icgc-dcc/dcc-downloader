package org.icgc.dcc.downloader.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

public class ExportedDataFileSystemTest {
	private static ExportedDataFileSystem fs;

	@BeforeClass
	public static void setup() throws IOException {
		URL resource = ExportedDataFileSystem.class.getClassLoader()
				.getResource("data");
		System.out.println(resource.getFile());
		fs = new ExportedDataFileSystem(resource.getFile());
	}

	@Test
	public void testInvalidFile() throws IOException {
		assertFalse(fs.isFile(new File("")));
		assertFalse(fs.isFile(new File("/")));
		assertFalse(fs.isFile(new File("DOES_NOT_EXIST")));
	}

	@Test
	public void testFileContent() throws IOException {
		File testfile = new File("testfile.txt");
		InputStream is = fs.createInputStream(new File("testfile.txt"), 0);
		long length = fs.getSize(testfile);
		byte[] data = new byte[(int) length];
		is.read(data);
		is.close();
		assertEquals("test", new String(data, "UTF-8"));
	}

	@Test
	public void testValidFile() throws IOException {
		assertTrue(fs.isFile(new File("testfile.txt")));
		assertTrue(fs.isFile(new File("/testfile.txt")));
		assertTrue(fs.isFile(new File("/project/testfile.txt")));
	}

	@Test
	public void testListFiles() throws IOException {
		List<File> files = fs.listFiles(new File("/project"));
		assertEquals(1, files.size());
		assertEquals("testfile.txt", files.get(0).getName());
	}

	@Test
	public void testValidDirectory() throws IOException {
		assertTrue(fs.isDirectory(new File("")));
		assertTrue(fs.isDirectory(new File("/")));
		assertTrue(fs.isDirectory(new File("/project")));
	}
}