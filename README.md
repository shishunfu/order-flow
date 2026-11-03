# OrderFlow

订单与库存管理系统，用于持续 12 周的 Java 工程能力实践。

## 技术基线
- Java 21
- Spring Boot 4.1.2
- Maven
- Spring MVC、Validation、JDBC、Actuator
- MySQL 8.4.8、Flyway、JUnit

## 环境
```bash
PS order-flow (main)> java -version
openjdk version "25.0.2" 2026-01-20 LTS
OpenJDK Runtime Environment (build 25.0.2+12-LTS)
OpenJDK 64-Bit Server VM (build 25.0.2+12-LTS, mixed mode, sharing)

PS order-flow (main)> mvn -v
Apache Maven 3.9.12 (848fbb4bf2d427b72bdb2471c22fced7ebd9a7a1)
Maven home: C:\ProgramFiles\Maven\apache-maven-3.9.12
Java version: 25.0.2, vendor: BellSoft, runtime: C:\ProgramFiles\Jdk\jdk25
Default locale: zh_CN, platform encoding: UTF-8
OS name: "windows 11", version: "10.0", arch: "amd64", family: "windows"

PS order-flow (main)> docker compose -f deploy/docker-compose.yml ps
NAME               IMAGE         COMMAND                   SERVICE   CREATED          STATUS                    PORTS
order-flow-mysql   mysql:8.4.8   "docker-entrypoint.s…"   mysql     47 minutes ago   Up 47 minutes (healthy)   0.0.0.0:3306->3306/tcp, [::]:3306->3306/tcp
```

## 启动
### 本地开发
```bash
docker compose -f deploy/docker-compose.yml up -d
```
### 本地运行
```bash
mvn spring-boot:run
```

健康检查：`http://localhost:8080/actuator/health`

## API 示例
```bash
curl -X POST http://localhost:8080/api/orders ^
  -H "Content-Type: application/json" ^
  -H "Idempotency-Key: demo-order-001" ^
  -d "{\"productId\":1001,\"quantity\":2}"
curl http://localhost:8080/api/orders/1
```

数据库默认配置：localhost:3306 / order_flow / orderflow / orderflow_dev。
可通过 `DB_URL`、`DB_USERNAME`、`DB_PASSWORD` 环境变量覆盖。

## 设计约定
- 第一阶段采用模块化单体，不提前拆微服务。
- `order` 和 `inventory` 各自拥有自己的业务规则和 Repository。
- 实验代码放 `src/test`，过程和证据放 `docs/experiments`。
- 每周更新 `docs/weekly-reports`，重要技术取舍写入 ADR。

## 当前边界
这是学习项目初始骨架，不是生产就绪系统。尚未实现订单取消、库存释放、支付、认证授权、限流、消息 Outbox、Redis、MQ 和完整可观测性；这些能力按周逐步增加。
