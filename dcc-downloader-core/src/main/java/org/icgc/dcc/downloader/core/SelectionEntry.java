package org.icgc.dcc.downloader.core;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SelectionEntry<K, V> {

	private K key;
	private V value;

	public SelectionEntry() {
		this.key = null;
		this.value = null;
	}
}
