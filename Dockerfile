FROM openjdk:21-jdk-slim
ENV LANG C.UTF-8
WORKDIR /application
COPY ./*.jar application.jar
EXPOSE 1883
EXPOSE 9995
ENV TZ=Asia/Shanghai
ENTRYPOINT exec java $JAVA_OPTS -jar -Dfile.encoding=UTF-8 -Dio.netty.leakDetectionLevel=DISABLED application.jar