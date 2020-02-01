package cassdemo;

import java.io.*;
import java.util.Properties;

import cassdemo.backend.Runner;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Main {

	public static void main(String[] args) throws IOException, BackendException {

		for (int i = 0; i < 3; i++) {
			Runner runner = new Runner();
			runner.run();
		}

		System.exit(0);

	}
}
