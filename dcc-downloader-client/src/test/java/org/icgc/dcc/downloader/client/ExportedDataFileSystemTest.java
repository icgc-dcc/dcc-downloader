/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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