package com.soebes.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

public class TapeLogReader {
    private Counters context;
    public TapeLogReader() {
        setContext(new Counters());
    }
    public void process(String strLine) {
        String[] columns = strLine.split("[ ]+");
        if (strLine.startsWith("tar: /dev/nst0")) {
            getContext().getCounter(ContentType.EMPTY).increment(1);
            return;
        }

        if (columns[2].startsWith("-")) {
            long sizeInBytes = Long.parseLong(columns[4]);
            getContext().getCounter(ContentType.BYTES).increment(sizeInBytes);

            getContext().getCounter(ContentType.FILES).increment(1);
        }

        if (columns[2].startsWith("d")) {
            context.getCounter(ContentType.DIRECTORIES).increment(1);
        }

        if (columns[2].startsWith("l")) {
            getContext().getCounter(ContentType.LINKS).increment(1);
        }
        if (columns[2].startsWith("V")) {
            getContext().getCounter(ContentType.VOLUMNHEADER).increment(1);
        }

    }

    public void read(File logFile) {
        try {
            BufferedReader in = new BufferedReader(new FileReader(logFile));
            String str;
            while ((str = in.readLine()) != null) {
                process(str);
            }
            in.close();
        } catch (IOException e) {
        }
    }

    public static void main(String [] args) {
        TapeLogReader rlr = new TapeLogReader();
        Date started = new Date();
        System.out.println("Started at: " + started);
        rlr.read(new File(args[0]));
        Date ended = new Date();
        System.out.println("Stopped at: " + ended);
        System.out.println("Runtime: " + ((ended.getTime()-started.getTime())/1000.0) + " seconds");
        for (ContentType item : ContentType.values()) {
            Counter counter = rlr.getContext().getCounter(item);
            System.out.println("Counter: " + item.name() + " value:" + counter.getValue());
        }
    }

    public void setContext(Counters context) {
        this.context = context;
    }
    public Counters getContext() {
        return context;
    }
}
