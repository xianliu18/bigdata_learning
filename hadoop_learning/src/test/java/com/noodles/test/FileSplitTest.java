package com.noodles.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import org.junit.Test;

/**
 * @description: TODO
 * @author: liuxian
 * @date: 2023-10-18 08:28
 */
public class FileSplitTest {

    @Test
    public void testPhoneSplit() throws IOException {
        String path = this.getClass().getClassLoader().getResource("phone_data.txt").getPath();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String str = null;
        while ((str = br.readLine()) != null) {
            String[] phoneArr = str.split("\t");
            System.out.println("phone: " + phoneArr[1] + " up = " + phoneArr[phoneArr.length - 3] + " down = " + phoneArr[phoneArr.length - 2]);
        }
        if (br != null) {
            br.close();
        }
    }

}
