import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.eclipse.paho.client.mqttv3.MqttConnectOptions.MQTT_VERSION_3_1_1;

public class MqttDemoStarter {
    public static void main(String[] args) throws MqttException, InterruptedException {
        
        String deviceId = "f41b85f4-3651-42e2-ab5a-fb4029ddcec4";
        String appId = "vle7j0";
        /**
         * 设置接入点，进入console管理平台获取
         */
        String endpoint = "vle7j0.cn1.mqtt.chat";

        /**
         * MQTT客户端ID，由业务系统分配，需要保证每个TCP连接都不一样，保证全局唯一，如果不同的客户端对象（TCP连接）使用了相同的clientId会导致连接异常断开。
         * clientId由两部分组成，格式为DeviceID@appId，其中DeviceID由业务方自己设置，appId在控console控制台创建，，clientId总长度不得超过64个字符。
         */
        String clientId = deviceId + "@" + appId;
        
        String username = "test";
        
        String appClientId = "YXA67-uKaalmThCOut6Q8uPLSg";
        String appClientSecret = "YXA63CFpMQFai4MdTDdGN92BBoG6_6g";
        
        // 获取token的URL
        //https://{restapi}/openapi/rm/app/token
        // 获取token
        String token = "";
        // 取token
        try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpPost httpPost = new HttpPost("https://{restapi}/openapi/rm/app/token");
            Map<String, String> params = new HashMap<>();
            params.put("appClientId", appClientId);
            params.put("appClientSecret", appClientSecret);
            //设置请求体参数
            StringEntity entity = new StringEntity(JSONObject.toJSONString(params), Charset.forName("utf-8"));
            entity.setContentEncoding("utf-8");
            httpPost.setEntity(entity);
            //设置请求头部
            httpPost.setHeader("Content-Type", "application/json");
            //执行请求，返回请求响应
            try (final CloseableHttpResponse response = httpClient.execute(httpPost)) {
                //请求返回状态码
                int statusCode = response.getStatusLine().getStatusCode();
                //请求成功
                if (statusCode == HttpStatus.SC_OK && statusCode <= HttpStatus.SC_TEMPORARY_REDIRECT) {
                    //取出响应体
                    final HttpEntity entity2 = response.getEntity();
                    //从响应体中解析出token
                    String responseBody = EntityUtils.toString(entity2, "utf-8");
                    JSONObject jsonObject = JSONObject.parseObject(responseBody);
                    token = jsonObject.getJSONObject("body").getString("access_token");
                } else {
                    //请求失败
                    throw new ClientProtocolException("请求失败，响应码为：" + statusCode);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String mqtt_token = "";
        // 取token
        try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpPost httpPost = new HttpPost("https://{restapi}/openapi/rm/user/token");
            Map<String, String> params = new HashMap<>();
            params.put("username", username);
            params.put("cid", clientId);
            //设置请求体参数
            StringEntity entity = new StringEntity(JSONObject.toJSONString(params), Charset.forName("utf-8"));
            entity.setContentEncoding("utf-8");
            httpPost.setEntity(entity);
            //设置请求头部
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Authorization", token);
            //请求响应
            try (final CloseableHttpResponse response = httpClient.execute(httpPost)) {
                //请求返回状态码
                int statusCode = response.getStatusLine().getStatusCode();
                //请求成功
                if (statusCode == HttpStatus.SC_OK && statusCode <= HttpStatus.SC_TEMPORARY_REDIRECT) {
                    //取出响应体
                    final HttpEntity entity2 = response.getEntity();
                    //从响应体中解析出token
                    String responseBody = EntityUtils.toString(entity2, "utf-8");
                    JSONObject jsonObject = JSONObject.parseObject(responseBody);
                    mqtt_token = jsonObject.getJSONObject("body").getString("access_token");
                } else {
                    //请求失败
                    throw new ClientProtocolException("请求失败，响应码为：" + statusCode);
                }
            }
            //执行请求，返回请求响应
        } catch (IOException e) {
            e.printStackTrace();
        }

        /**
         * 需要订阅或发送消息的topic名称
         * 如果使用了没有创建或者没有被授权的Topic会导致鉴权失败，服务端会断开客户端连接。
         */
        final String myTopic = "myTopic";

        /**
         * QoS参数代表传输质量，可选0，1，2。详细信息，请参见名词解释。
         */
        final int qosLevel = 0;
        final MemoryPersistence memoryPersistence = new MemoryPersistence();

        /**
         * 客户端协议和端口。客户端使用的协议和端口必须匹配，如果是ws或者wss使用http://，如果是mqtt或者mqtts使用tcp://
         */
        final MqttClient mqttClient = new MqttClient("tcp://" + endpoint + ":1883", clientId, memoryPersistence);
        /**
         * 设置客户端发送超时时间，防止无限阻塞。
         */
        mqttClient.setTimeToWait(5000);

        final ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        mqttClient.setCallback(new MqttCallbackExtended() {
            /**
             * 连接完成回调方法
             * @param b
             * @param s
             */
            @Override
            public void connectComplete(boolean b, String s) {
                /**
                 * 客户端连接成功后就需要尽快订阅需要的Topic。
                 */
                System.out.println("connect success");
                executorService.submit(() -> {
                    try {
                        final String[] topicFilter = {myTopic};
                        final int[] qos = {qosLevel};
                        mqttClient.subscribe(topicFilter, qos);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            /**
             * 连接失败回调方法
             * @param throwable
             */
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("connection lost");
                throwable.printStackTrace();
            }

            /**
             * 接收消息回调方法
             * @param s
             * @param mqttMessage
             */
            @Override
            public void messageArrived(String s, MqttMessage mqttMessage) {
                System.out.println("receive msg from topic " + s + " , body is " + new String(mqttMessage.getPayload()));
            }

            /**
             * 发送消息回调方法
             * @param iMqttDeliveryToken
             */
            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                System.out.println("send msg succeed topic is : " + iMqttDeliveryToken.getTopics()[0]);
            }
        });
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        /**
         * 用户名，在console中注册
         */
        mqttConnectOptions.setUserName(username);
        /**
         * 用户密码为第一步中申请的token
         */
        mqttConnectOptions.setPassword(mqtt_token.toCharArray());
        mqttConnectOptions.setCleanSession(true);
        mqttConnectOptions.setKeepAliveInterval(90);
        mqttConnectOptions.setAutomaticReconnect(true);
        mqttConnectOptions.setMqttVersion(MQTT_VERSION_3_1_1);
        mqttConnectOptions.setConnectionTimeout(5000);

        mqttClient.connect(mqttConnectOptions);
        //暂停1秒钟，等待连接订阅完成
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            MqttMessage message = new MqttMessage("hello world pub sub msg".getBytes());
            message.setQos(qosLevel);
            /**
             * 发送普通消息时，Topic必须和接收方订阅的Topic一致，或者符合通配符匹配规则。
             */
            mqttClient.publish(myTopic, message);
        }
        Thread.sleep(1000);
        mqttClient.unsubscribe(new String[]{myTopic});
        Thread.sleep(Long.MAX_VALUE);
    }
}
