package com.justplay1994.github.polygonmatch;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

public class DBOperateService {
    private static final Logger logger = LoggerFactory.getLogger(DBOperateService.class);

    private static String DBTYPE;

    private static final String MYSQL="MYSQL";
    private static final String ORACLE="ORACLE";

    private static HikariDataSource dataSource;
    public static void init(Properties properties){
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.get("URL").toString());
        config.setUsername(properties.get("USER").toString());
        config.setPassword(properties.get("PASSWORD").toString());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");



        dataSource = new HikariDataSource(config);
        String type = dataSource.getJdbcUrl().split(":")[1];
        if (type.equals("mysql"))
            DBTYPE=MYSQL;
        if (type.equals("oracle"))
            DBTYPE=ORACLE;
    }

    /**
     * 通过表名，查询该表所有数据
     * @param tbName
     * @return
     */
    public List<LinkedHashMap> queryAllDataByTableName(String tbName){

        List<LinkedHashMap> list  = new ArrayList<LinkedHashMap>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        String sql = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();

            if (DBTYPE.equals(MYSQL))
                sql = "select * from "+tbName;
            if (DBTYPE.equals(ORACLE))
                sql = "select * from \""+tbName+"\"";
            logger.debug("sql: "+sql);
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnNumber= resultSetMetaData.getColumnCount();
                LinkedHashMap hashMap = new LinkedHashMap();
                for (int i = 1; i <= columnNumber; ++i){
                    String colName = resultSetMetaData.getColumnName(i);
                    String typeName = resultSetMetaData.getColumnTypeName(i);
                    String value = resultSet.getString(i);
//                    logger.debug("colName: "+colName);
//                    logger.debug("typeName: "+ typeName);
//                    logger.debug("value: "+ value);

                    hashMap.put(colName,value);
                }
                list.add(hashMap);
            }
        } catch (SQLException e) {
            logger.error("database connection error!\n",e);
        } finally {
            if (connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("connection close error!\n",e);
                }
            }
            if (statement!=null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("statement close error!sql: "+sql+"\n",e);
                }
            }
            if (resultSet!=null){
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    logger.error("resultSet close error!\n",e);
                }
            }
        }
        return list;
    }

    /**
     * 新增表字段,默认类型为VARCHAR（32） NULL
     * @param tbName
     * @param colName
     */
    public boolean addColumn(String tbName, String colName, String comment){
        boolean result = false;
        Connection connection = null;
        Statement statement = null;
        String sql = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            if (DBTYPE.equals(MYSQL))
                sql = "alter table "+tbName+" add "+colName+" VARCHAR(32) NULL COMMENT '"+comment+"'";
            if (DBTYPE.equals(ORACLE))
                sql = "alter table \""+tbName+"\" add \""+colName+"\" NVARCHAR2 ( 100 ) NULL ";
            logger.debug("sql: "+sql);
            result = statement.execute(sql);
            if (DBTYPE.equals(ORACLE))
                sql= "comment on column \""+tbName+"\".\""+colName+"\" is '"+comment+"'";
            logger.debug("sql: "+sql);
            result = statement.execute(sql);
        } catch (SQLException e) {
            if (e.getMessage().split(" ")[0].equalsIgnoreCase("Duplicate")){
                result = true;
            }else{
                logger.error("update database error! sql: "+sql+"\n",e);
            }
        }finally {
            close(connection,statement);
        }
        return result;
    }

    /**
     * 修改一条数据的一个字段的值
     * @param tbName
     * @param id
     * @param colName
     * @param colValue
     * @return
     */
    public boolean updateDataColValue(String tbName, String id, String colName, String colValue){
        boolean result = false;
        Connection connection = null;
        Statement statement = null;
        String sql = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            if (DBTYPE.equals(MYSQL))
                sql = "UPDATE "+tbName+" SET "+colName+" = '"+colValue+"' WHERE id = "+id;
            if (DBTYPE.equals(ORACLE))
                sql = "UPDATE \""+tbName+"\" set \""+colName+"\" = '"+colValue+"' WHERE \"id\" = "+id;
            logger.debug("sql: "+sql);
            result = statement.execute(sql);
        } catch (SQLException e) {
            logger.error("update database error!sql: "+sql+"\n",e);
        }finally {
            close(connection,statement);
        }
        return result;
    }

    /**
     * 执行sql语句
     * @return
     */
    public boolean executeSql(String sql){
        boolean result = false;
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            logger.debug("sql: "+sql);
            result = statement.execute(sql);
        } catch (SQLException e) {
            logger.error("update database error!\n",e);
        }finally {
            close(connection,statement);
        }
        return result;
    }

    public boolean close(Connection connection, Statement statement){
        boolean result = false;
        boolean connectionResult = false;
        boolean statementResult = false;
        if (connection!=null){
            try {
                connection.close();
                connectionResult = true;
            } catch (SQLException e) {
                logger.error("connection close error!\n",e);
            }
        }
        if (statement!=null){
            try {
                statement.close();
                statementResult = true;
            } catch (SQLException e) {
                logger.error("statement close error!\n",e);
            }
        }
        if (connectionResult && statementResult)
            result = true;
        return result;
    }
}
