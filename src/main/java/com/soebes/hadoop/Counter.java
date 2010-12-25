package com.soebes.hadoop;

public class Counter {

	private Long value;

	public Counter() {
		setValue(0L);
	}

	public void increment(long count) {
		setValue(getValue() + count);
	}

	public Long getValue() {
		return value;
	}
	public void setValue(Long value) {
		this.value = value;
	}
	
	
}
