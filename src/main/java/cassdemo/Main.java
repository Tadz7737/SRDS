package cassdemo;

import java.io.*;

import cassdemo.backend.Runner;

import cassdemo.backend.BackendException;

public class Main {

	public static void main(String[] args) throws IOException, BackendException {

		for (int i = 0; i < 500; i++) {
			Runner runner = new Runner();
			runner.start();
			try {
				runner.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.exit(0);

	}
}
