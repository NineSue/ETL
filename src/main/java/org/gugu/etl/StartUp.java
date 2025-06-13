package org.gugu.etl;

import cn.hutool.core.io.FileUtil;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.FileSystems;

@Component
public class StartUp {
    public static String dataDir = null;
    // 系统文件分隔符
    public static String fileSeparator = FileSystems.getDefault()
                                                    .getSeparator();
    public static String  getPathString(String fileName) {
        return dataDir + fileSeparator + fileName;
    }
    
    @PostConstruct
    void dataDirInit() {
        // 数据文件存放在用户目录下的.etl/process_files/文件夹下
        StringBuilder sb = new StringBuilder(System.getProperty("user.home"));
        sb.append(fileSeparator)
          .append(".etl")
          .append(fileSeparator)
          .append("process_files");
        dataDir = sb.toString();
        if (! FileUtil.exist(dataDir)) {
            FileUtil.mkdir(dataDir);
            System.out.println("数据目录创建成功！");
        } else {
            System.out.println("数据目录已存在");
        }
    }
}
