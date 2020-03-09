package com.aifeifeng.mr3;

import org.apache.hadoop.util.ToolRunner;

/**
 * PACKAGE_NAMW   com.aifeifeng.mr3
 * DATE      12
 * Author     Crush
 */
public class FileTableToolApplication2 {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FileTableTool2(),args);
    }
}
