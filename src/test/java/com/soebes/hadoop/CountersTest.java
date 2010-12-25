package com.soebes.hadoop;

import junit.framework.Assert;

import org.testng.annotations.Test;


public class CountersTest {

	@Test
	public void firstTest() {
		Counters c = new Counters();
		c.getCounter(ContentType.EMPTY).increment(1);
		Assert.assertEquals(c.getCounter(ContentType.EMPTY).getValue(), new Long(1));
	}

	@Test
	public void secondTest() {
		Counters c = new Counters();
		c.getCounter(ContentType.BYTES).increment(242234L);
		Assert.assertEquals(c.getCounter(ContentType.BYTES).getValue(), new Long(242234L));
		Assert.assertEquals(c.getCounter(ContentType.EMPTY).getValue(), new Long(0));
		Assert.assertEquals(c.getCounter(ContentType.DIRECTORIES).getValue(), new Long(0));
		Assert.assertEquals(c.getCounter(ContentType.FILES).getValue(), new Long(0));
		Assert.assertEquals(c.getCounter(ContentType.LINKS).getValue(), new Long(0));
		Assert.assertEquals(c.getCounter(ContentType.VOLUMNHEADER).getValue(), new Long(0));

		c.getCounter(ContentType.FILES).increment(5L);
		Assert.assertEquals(c.getCounter(ContentType.FILES).getValue(), new Long(5));

		c.getCounter(ContentType.FILES).increment(1L);
		Assert.assertEquals(c.getCounter(ContentType.FILES).getValue(), new Long(6));
	}
}
