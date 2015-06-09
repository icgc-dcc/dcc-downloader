/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.downloader.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.icgc.dcc.downloader.core.DonorIdEncodingUtil;
import org.icgc.dcc.downloader.core.SchemaUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * 
 */
public class DonorIdEncodingUtilTest {

	@Test
	public void testDonorIdsCodec1Valid() {
		ImmutableSet.Builder<String> expectedBuilder = ImmutableSet.builder();
		Random ran = new Random();

		int MAX_NUM_ID = 1000;
		for (int i = 0; i < MAX_NUM_ID; ++i) {
			expectedBuilder.add(SchemaUtil.decodeDonorId(ran
					.nextInt(Integer.MAX_VALUE / 2)));
		}
		Set<String> expectedDonorIds = expectedBuilder.build();

		String code = DonorIdEncodingUtil.encodeDonorIds(expectedDonorIds);
		List<Integer> actualIds = DonorIdEncodingUtil.convertCodeToList(code);

		ImmutableSet.Builder<String> actualBuilder = ImmutableSet.builder();
		for (Integer id : actualIds) {
			actualBuilder.add(SchemaUtil.decodeDonorId(id));
		}
		ImmutableSet<String> actualDonorIds = actualBuilder.build();
		assertThat(actualDonorIds).containsAll(expectedDonorIds).hasSameSizeAs(
				expectedDonorIds);

	}

	@Test(expected = NumberFormatException.class)
	public void testDonorIdsCodec1InValid() {
		DonorIdEncodingUtil.encodeDonorIds(ImmutableSet.<String> of("DO___"));
	}
}
