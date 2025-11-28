## MQTTY 
### 简述
MQTT BROKER中间件，Netty开发，遵守MQTT3.1.1规范实现。

### 技术选型
- Springboot 3
- Netty 4
- JDK 21(最低可以降级到JDK17)

### 项目结构
```
├─amqp 消息中间件相关支持，可自定义
├─broker BROKER相关支持
│  ├─codec 编解码器
│  ├─config 配置
│  ├─constants 常量类
│  ├─handler 
│  └─protocol MQTT协议相关
├─common 公共模块
│  ├─auth 授权相关
│  ├─message 消息相关
│  ├─session Session相关
│  └─subscribe 订阅相关
├─service 外部服务
│  ├─evt 封装一些事件
│  │  └─enums
│  ├─impl 
│  └─vo
├─store MQTT服务器会话信息
│  ├─cache
│  ├─message
│  ├─session
│  ├─subscribe
│  └─util
└─utils 公共工具包

```

### 功能实现
1. MQTT3.1.1规范实现
2. 完整的QoS服务质量等级实现
3. 遗嘱消息, 保留消息及消息分发重试
3. 心跳机制
4. MQTT连接认证(可选择是否开启, 包括SSL/TLS认证和账号密码认证)
5. 主题过滤(支持通配符订阅，#可订阅本级和所有子级, +可订阅下一层级主题)
7. Websocket支持(可选择是否开启)

### 快速开始
#### 源码部署
1. clone源码
2. mvn clean & package
3. java -jar app.jar运行
#### Docker容器部署
Dockerfile
```
FROM openjdk:21-jdk-slim
ENV LANG C.UTF-8
WORKDIR /application
COPY ./*.jar application.jar
EXPOSE 1883
ENV TZ=Asia/Shanghai
ENTRYPOINT exec java $JAVA_OPTS -jar -Dfile.encoding=UTF-8 -Dio.netty.leakDetectionLevel=ADVANCED application.jar
```

docker-compose.yaml
```
version: '3.8'
services:
  mqtt-broker:
    container_name: mqtt-broker
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./log:/application/log
    ports:
      - "1883:1883"
    restart: always
```

### 附录
#### 生成自签名SSL证书

生成CA证书

```openssl req -new -x509 -keyout ca.key -out ca.crt -days 36500```

生成服务端和客户端私钥

```openssl genrsa -des3 -out server.key 1024```

```openssl genrsa -des3 -out client.key 1024```

根据 key 生成 csr 文件

```openssl req -new -key server.key -out server.csr```

```openssl req -new -key client.key -out client.csr```

根据配置生成通用证书[增加对特定域名和IP的校验]

```openssl x509 -req -days 36500 -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -extfile san.cnf -extensions req_ext```

```openssl x509 -req -days 36500 -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -extfile san.cnf -extensions req_ext```

将 key 文件进行 PKCS#8 编码

```openssl pkcs8 -topk8 -in server.key -out pkcs8_server.key -nocrypt```

```openssl pkcs8 -topk8 -in client.key -out pkcs8_client.key -nocrypt```

最后得到

服务器端： `ca.crt、server.crt、pkcs8_server.key`

客户端端： `ca.crt、client.crt、pkcs8_client.key`

附加：将 RSA 私钥转换为 PKCS#8 格式
``` shell
openssl pkcs8 -topk8 -in client.key -out client.pem -nocrypt
```

san.cnf 文件内容如下：
```
[ req ]
default_bits        = 2048
prompt              = no
default_md          = sha256
req_extensions      = req_ext
distinguished_name  = dn

[ dn ]
C = CN
ST = SH
L = San Francisco
O = sinognss
CN = 你的域名  # 这是证书的 Common Name (CN)

[ req_ext ]
subjectAltName = @alt_names  # 正确引用 alt_names 部分

[ alt_names ]
DNS.1 = cloud.sinognss.com   # 指定域名
DNS.2 = localhost            # 本地域名
IP.1 = 127.0.0.1             # 本地IP
IP.2 = 192.168.2.13          # 你的IP1
IP.3 = 192.168.2.12          # 你的IP2
```
