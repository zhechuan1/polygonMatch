package com.justplay1994.github.polygonmatch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 给带有经纬度数据，加上所属街道或所属社区的标签
 */
public class AddStreet {
    private static final Logger logger = LoggerFactory.getLogger(AddStreet.class);

    static List<HashMap<String, String>> streetHashMapList;

    static long total = 0;/*总数量*/
    static long totalHit = 0; /*命中数量*/

    /**
     * 初始化：将街道、网格、社区数据放入es中
     */
    public void init() throws IOException {

        initStreet();
    }

    /**
     * 初始化街道，将街道geojson加载至es中
     *
     * @throws IOException
     */
    private void initStreet() throws IOException {

//        File file = new File("street.json");
        InputStream inputStream = AddStreet.class.getResourceAsStream("/street.json");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(inputStream);
        JsonNode features = rootNode.path("features");
        streetHashMapList = new ArrayList<HashMap<String, String>>();
        for (int i = 0; i < features.size(); ++i) {
            String streetName = features.get(i).get("properties").get("name").asText();
            JsonNode coordinates = features.get(i).get("geometry").get("coordinates");
            String id = String.valueOf(features.get(i).get("id"));

            /*第一个多边形，主多边形*/
            String points = coordinates.get(0).toString();

            HashMap hashMap = new HashMap();
            hashMap.put("name", streetName);
            hashMap.put("points", points);
            streetHashMapList.add(hashMap);

//            MyURLConnection myURLConnection = new MyURLConnection();
//            String body = "{\n" +
//                    "  \"name\" : \""+streetName+"\",\n" +
//                    "  \"location\" : {\n" +
//                    "      \"type\" : \"polygon\",\n" +
//                    "      \"coordinates\" : "+
//                    coordinates.toString()+
//                    "  }\n" +
//                    "}";
//            myURLConnection.request(ESOperate.esURL+streetIndex+"/_doc/"+id,"POST",body);
        }
    }

    /**
     * 多线程
     * 遍历所有街道，查询在每个街道几何多边形中的点集合
     */
   public void addStreet(List<String> tables) throws SQLException, IOException {
       DBOperateService dbOperateService = new DBOperateService();
        long start = System.currentTimeMillis();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8,
                8,
                100,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(8)     //等待队列
        );
        total = 0;
        totalHit = 0;
        for (int i = 0; i < tables.size(); ++i) {
            String tbName = tables.get(i);
            if (streetHashMapList == null)
                init();
            dbOperateService.addColumn(tbName,"street","街道");
            List<LinkedHashMap> linkedHashMaps = dbOperateService.queryAllDataByTableName(tbName);
            for (int k = 0; k < linkedHashMaps.size(); ++k) {/*遍历表数据*/
                logger.info("[Tables "+tbName+" : "+((float)i/(float)(tables.size()-1))*100+"%]"+"[Datas : "+((float)k/(float) (linkedHashMaps.size()-1))*100+"%]");
                String jd84 = linkedHashMaps.get(k).get("jd84").toString();
                String wd84 = linkedHashMaps.get(k).get("wd84").toString();
                String id = linkedHashMaps.get(k).get("id").toString();
                total++;
                executor.execute(new Thread(new AddStreetThread(jd84,wd84,dbOperateService,tbName,id)));
                /*如果当前线程数达到最大值，则阻塞等待*/
                while(executor.getQueue().size()>=executor.getMaximumPoolSize()){
                    logger.debug("Thread waite ...Already maxThread. Now Thread nubmer:"+executor.getActiveCount());
//                            logger.debug("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+executor.getQueue().size()+"，已执行完别的任务数目："+executor.getCompletedTaskCount());
                    long time = 100;
                    try {
                        Thread.sleep(time);
                    } catch (InterruptedException e) {
                        logger.error("sleep error!",e);
                    }
                }
            }
        }
        logger.info("pology:"+streetHashMapList.size());
        logger.info("totalHit:" + totalHit);
        logger.info("total:" + total);

        /*等待线程执行结束*/
        while(executor.getActiveCount()!=0 || executor.getQueue().size()!=0){
            try {
                Thread.sleep(1000);
                logger.debug("active thread count: "+executor.getActiveCount());
            } catch (InterruptedException e) {
                logger.error("sleep error!\n",e);
            }
        }
        /*关闭线程池*/
        executor.shutdown();
        long end = System.currentTimeMillis();
        long second = ((end - start) / 1000) % 60;
        long minite = (end - start) / 1000 / 60 % 60;
        logger.info("spend time : " + minite + " m" + second + " s");
    }
}
