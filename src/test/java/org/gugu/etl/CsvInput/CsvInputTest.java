package org.gugu.etl.CsvInput;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import plugin.input.CsvInput;
import runtask.Step;
import runtask.StepList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;

public class CsvInputTest {
    @Test
    public void testCsvInput() throws Exception {
        String path = "E:\\StudySoft\\学习\\ETL\\1GB.csv";
        Step input = new Step();
        input.withStepId(1)
             .withDes("读取csv文件")
             .withDomain("input")
             .withSubType("csv")
//             .withConfig("filePath", "C:\\Users\\白乃常\\Desktop\\custom_data.csv")
             .withConfig("filePath", path)
             .withConfig("delimiter", ",")
             .withConfig("quoteChar", "\"")
             .withConfig("hasHeader", true);

        Step output = new Step();
        output.withStepId(2)
              .withDes("空操作")
              .withDomain("output")
              .withSubType("nop")
              .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));

        new Scheduler(stepList).execute();
    }
    
    
    
    public static String getHexValueAtPosition(String  file, long position) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // 定位到指定位置
            raf.seek(position);
            
            // 读取一个字节
            int byteValue = raf.read();
            
            // 如果到达文件末尾，返回 null 或者抛出异常，根据需求处理
            if (byteValue == -1) {
                return null;
            }
            
            // 转换为十六进制字符串
            return String.format("%02X", byteValue);
        }
    }
    
    private static void printFile(String path) throws Exception {
        try (RandomAccessFile reader = new RandomAccessFile(path, "r")) {
            int bytesRead;
            byte[] buffer = new byte[16];
            
            long filePointer = 0;
            while ((bytesRead = reader.read(buffer)) != - 1) {
                // Print the current position in the file
                System.out.printf("%08d: ", filePointer);
                
                // Print hex values
                for (int i = 0; i < bytesRead; i++) {
                    System.out.printf("%02X ", buffer[i]);
                }
                
                // Print ASCII representation
                System.out.print(" |");
                for (int i = 0; i < bytesRead; i++) {
                    char c = (char) (buffer[i] & 0xFF);
                    if (Character.isISOControl(c)) {
                        System.out.print('.');
                    } else {
                        System.out.print(c);
                    }
                }
                System.out.println("|");
                
                filePointer += bytesRead;
            }
        }
    }
    
}
