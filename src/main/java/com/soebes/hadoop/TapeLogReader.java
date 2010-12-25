package com.soebes.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TapeLogReader {
	private Counters context;
	public TapeLogReader() {
		setContext(new Counters());
	}
	public void process(String strLine) {
        String[] columns = strLine.split("[ ]+");
        if (strLine.startsWith("tar: /dev/nst0")) {
//			tar: /dev/nst0: Cannot open: Input/output error
//			tar: Error is not recoverable: exiting now
            getContext().getCounter(ContentType.EMPTY).increment(1);
            return;
        }

        if (columns[2].startsWith("-")) {
            //We only count files, but no directories or links etc.
            long sizeInBytes = Long.parseLong(columns[4]);
            getContext().getCounter(ContentType.BYTES).increment(sizeInBytes);

            getContext().getCounter(ContentType.FILES).increment(1);
        }

//		if (columns[2].startsWith("d")) {
//			context.getCounter(TapeLogFile.DIRECTORIES).increment(1);
//			context.write(new Text("directory"), new Text("Test"));
//		}

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
		rlr.read(new File(args[0]));
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
