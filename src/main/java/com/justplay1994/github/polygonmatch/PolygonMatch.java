package com.justplay1994.github.polygonmatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PolygonMatch {
    static Properties properties = new Properties();

    private static final Logger logger= LoggerFactory.getLogger(PolygonMatch.class);
    public static void main(String[] args){


        try {
            logger.info(PolygonMatch.class.getResource("").toString());
            logger.info(PolygonMatch.class.getResource("/").toString());
            InputStream in = PolygonMatch.class.getResourceAsStream("/polygonmatch.properties");
            properties.load(in);
            DBOperateService.init(properties);//初始化连接池

            String[] tables = properties.getProperty("tables").split(",");

            AddStreet addStreet = new AddStreet();
            addStreet.addStreet(Arrays.asList(tables));

        } catch (FileNotFoundException e) {
            logger.error("File polygonmatch.properties not found!\n",e);
        } catch (IOException e) {
            logger.error("inputstream error\n",e);
        } catch (SQLException e) {
            logger.error("table name format error\n",e);
        }
    }
}
