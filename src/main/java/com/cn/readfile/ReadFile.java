package com.cn.readfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadFile {
	public void readFile() {
		try {
			File file = new File("D:/h2cache.log");
			FileInputStream is = new FileInputStream(file);
			InputStreamReader streamReader = new InputStreamReader(is);
			BufferedReader reader = new BufferedReader(streamReader);
			while (reader.readLine() != null) {
				String line = reader.readLine();
				if(line != null && !"".equals(line)) {
					String regex = "\\【(.*?)】";
					Pattern pattern = Pattern.compile(regex);
					Matcher matcher = pattern.matcher(line);
					while (matcher.find()) {
						if(Integer.valueOf(matcher.group(1)) >4000) {
							System.out.println(line);
						}
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		ReadFile rf = new ReadFile();
		rf.readFile();
	}
}
