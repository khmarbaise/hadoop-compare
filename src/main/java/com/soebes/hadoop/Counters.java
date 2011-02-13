package com.soebes.hadoop;

import java.util.HashMap;

public class Counters {

    private HashMap<String, Counter> counters;

    public Counters() {
        counters = new HashMap<String, Counter>();
    }

    public Counter getCounter(Enum<?> empty) {
        String name = empty.getDeclaringClass().getName() + "$" + empty.name();
        if (!counters.containsKey(name)) {
            counters.put(name, new Counter());
        }
        return counters.get(name);
    }

}
